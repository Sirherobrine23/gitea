// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package mailer

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"gitea.dev/modules/setting"
	mailbox_service "gitea.dev/services/mailbox"
	sender_service "gitea.dev/services/mailer/sender"
)

type mailboxAwareSender struct {
	ctx      context.Context
	upstream sender_service.Sender
}

// sendUpstream hands a message to the [mailer] transport, which is absent when
// the instance relies solely on the integrated mailbox server.
func (s *mailboxAwareSender) sendUpstream(from string, to []string, msg io.WriterTo) error {
	if s.upstream == nil {
		return fmt.Errorf("cannot send mail to %v: no [mailer] transport is configured", to)
	}
	return s.upstream.Send(from, to, msg)
}

type mailboxRawMessage []byte

func (m mailboxRawMessage) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(m)
	return int64(n), err
}

func (s *mailboxAwareSender) Send(from string, to []string, msg io.WriterTo) error {
	if !setting.MailboxServer.Enabled || len(to) == 0 {
		return s.sendUpstream(from, to, msg)
	}
	var raw bytes.Buffer
	if _, err := msg.WriteTo(&raw); err != nil {
		return err
	}

	local := make([]string, 0, len(to))
	remote := make([]string, 0, len(to))
	for _, recipient := range to {
		if _, err := mailbox_service.ResolveRecipient(s.ctx, recipient); err == nil {
			local = append(local, recipient)
			continue
		}
		if mailbox_service.IsLocalAddress(recipient) {
			return fmt.Errorf("local mailbox recipient does not exist: %s", recipient)
		}
		remote = append(remote, recipient)
	}

	signedRaw, err := mailbox_service.SignOutboundDKIM(raw.Bytes())
	if err != nil {
		return fmt.Errorf("sign Gitea mail with DKIM: %w", err)
	}
	wire := mailboxRawMessage(signedRaw)
	// Relay first. Local delivery is Message-ID de-duplicated, so a queue retry
	// after a partial failure cannot create repeated local copies.
	if len(remote) > 0 {
		if setting.MailboxServer.OutboundMode == setting.OutboundModeDirect {
			// Direct mode owns remote delivery, including its own retries, so the
			// [mailer] transport is not consulted at all.
			if err := mailbox_service.SendRemote(s.ctx, 0, from, remote, signedRaw); err != nil {
				return err
			}
		} else if err := s.sendUpstream(from, remote, wire); err != nil {
			return err
		}
	}
	if len(local) > 0 {
		if _, err := mailbox_service.DeliverRaw(s.ctx, from, local, signedRaw, false); err != nil {
			return err
		}
	}
	return nil
}
