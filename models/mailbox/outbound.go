// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package mailbox

import (
	"context"
	"errors"
	"strings"

	"gitea.dev/models/db"
	"gitea.dev/modules/timeutil"
)

// MailOutbound is one queued remote delivery. A message with several remote
// recipients is stored as one row per recipient domain, which is the unit a
// single SMTP conversation can deliver and therefore the unit that can be
// retried, deferred or failed independently.
type MailOutbound struct {
	ID     int64  `xorm:"pk autoincr"`
	UserID int64  `xorm:"INDEX NOT NULL DEFAULT 0"`
	Domain string `xorm:"INDEX VARCHAR(255) NOT NULL"`

	FromAddress string `xorm:"VARCHAR(320) NOT NULL"`
	Recipients  string `xorm:"TEXT NOT NULL"`
	Raw         []byte `xorm:"LONGBLOB"`

	Attempts    int                `xorm:"NOT NULL DEFAULT 0"`
	LastError   string             `xorm:"TEXT"`
	LastCode    int                `xorm:"NOT NULL DEFAULT 0"`
	NextAttempt timeutil.TimeStamp `xorm:"INDEX NOT NULL"`

	CreatedUnix timeutil.TimeStamp `xorm:"created NOT NULL"`
	UpdatedUnix timeutil.TimeStamp `xorm:"updated NOT NULL"`
}

func (*MailOutbound) TableName() string { return "mailbox_outbound" }

func init() {
	db.RegisterModel(new(MailOutbound))
}

// Outbound keeps the package API readable; see the Message type alias comment.
type Outbound = MailOutbound

var ErrOutboundNotExist = errors.New("mailbox outbound message does not exist")

// RecipientList splits the stored recipient set.
func (o *MailOutbound) RecipientList() []string {
	if strings.TrimSpace(o.Recipients) == "" {
		return nil
	}
	return strings.Split(o.Recipients, "\n")
}

// SetRecipients stores the recipient set for one destination domain.
func (o *MailOutbound) SetRecipients(recipients []string) {
	o.Recipients = strings.Join(recipients, "\n")
}

func InsertOutbound(ctx context.Context, out *Outbound) error {
	if out.Domain == "" {
		return errors.New("outbound domain is required")
	}
	if out.NextAttempt == 0 {
		out.NextAttempt = timeutil.TimeStampNow()
	}
	_, err := db.GetEngine(ctx).Insert(out)
	return err
}

// ClaimDueOutbound returns queued deliveries whose next attempt has come due.
// The rows are pushed forward before they are handed out so a concurrent worker
// or a second instance cannot pick up the same delivery.
func ClaimDueOutbound(ctx context.Context, limit int, leaseUntil timeutil.TimeStamp) ([]*Outbound, error) {
	if limit <= 0 {
		return nil, nil
	}
	claimed := make([]*Outbound, 0, limit)
	err := db.WithTx(ctx, func(ctx context.Context) error {
		due := make([]*Outbound, 0, limit)
		if err := db.GetEngine(ctx).Where("next_attempt <= ?", timeutil.TimeStampNow()).
			Asc("next_attempt").Limit(limit).Find(&due); err != nil {
			return err
		}
		for _, out := range due {
			// The lease is a conditional update on the value that was read, so only
			// one worker can win a given row.
			updated, err := db.GetEngine(ctx).Where("id = ? AND next_attempt = ?", out.ID, out.NextAttempt).
				Cols("next_attempt").Update(&Outbound{NextAttempt: leaseUntil})
			if err != nil {
				return err
			}
			if updated == 1 {
				claimed = append(claimed, out)
			}
		}
		return nil
	})
	return claimed, err
}

// RescheduleOutbound records a failed attempt and sets the next retry time.
func RescheduleOutbound(ctx context.Context, id int64, attempts int, code int, lastError string, next timeutil.TimeStamp) error {
	if len(lastError) > 1024 {
		lastError = lastError[:1024]
	}
	_, err := db.GetEngine(ctx).Where("id = ?", id).Cols("attempts", "last_code", "last_error", "next_attempt").
		Update(&Outbound{Attempts: attempts, LastCode: code, LastError: lastError, NextAttempt: next})
	return err
}

func DeleteOutbound(ctx context.Context, id int64) error {
	_, err := db.GetEngine(ctx).Where("id = ?", id).Delete(new(Outbound))
	return err
}

// CountOutbound reports the queue depth, for the mail settings page.
func CountOutbound(ctx context.Context, userID int64) (int64, error) {
	return db.GetEngine(ctx).Where("user_id = ?", userID).Count(new(Outbound))
}
