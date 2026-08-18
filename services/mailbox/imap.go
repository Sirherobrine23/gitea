// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package mailbox

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	mailbox_model "gitea.dev/models/mailbox"
	user_model "gitea.dev/models/user"
	"gitea.dev/modules/log"
	"gitea.dev/modules/setting"
	"gitea.dev/modules/timeutil"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/backend"
	"github.com/emersion/go-imap/backend/backendutil"
	imapserver "github.com/emersion/go-imap/server"
	"github.com/emersion/go-message"
	"github.com/emersion/go-message/textproto"
)

const imapDelimiter = "/"

type imapBackend struct {
	ctx context.Context
}

type imapUser struct {
	ctx  context.Context
	user *user_model.User
}

type imapMailbox struct {
	ctx  context.Context
	user *user_model.User
	name string
}

var (
	_ backend.Backend = (*imapBackend)(nil)
	_ backend.User    = (*imapUser)(nil)
	_ backend.Mailbox = (*imapMailbox)(nil)
)

func initIMAP(ctx context.Context, tlsConfig *tls.Config) error {
	backend := &imapBackend{ctx: ctx}
	if addr := strings.TrimSpace(setting.MailboxServer.IMAPListen); addr != "" {
		server := newIMAPServer(backend, tlsConfig)
		server.Addr = addr
		ln, err := netListen("tcp", addr)
		if err != nil {
			return fmt.Errorf("listen on IMAP %s: %w", addr, err)
		}
		log.Info("Mailbox IMAP listening on %s", addr)
		go serveIMAP(ctx, server, ln)
	}
	if addr := strings.TrimSpace(setting.MailboxServer.IMAPSListen); addr != "" {
		if tlsConfig == nil {
			return fmt.Errorf("IMAPS listener %s requires TLS_CERT_FILE and TLS_KEY_FILE", addr)
		}
		server := newIMAPServer(backend, tlsConfig)
		server.Addr = addr
		ln, err := tls.Listen("tcp", addr, tlsConfig.Clone())
		if err != nil {
			return fmt.Errorf("listen on IMAPS %s: %w", addr, err)
		}
		log.Info("Mailbox IMAPS listening on %s", addr)
		go serveIMAP(ctx, server, ln)
	}
	return nil
}

func newIMAPServer(b backend.Backend, tlsConfig *tls.Config) *imapserver.Server {
	s := imapserver.New(b)
	s.TLSConfig = tlsConfig
	s.AllowInsecureAuth = setting.MailboxServer.AllowInsecureAuth
	s.AutoLogout = imapserver.MinAutoLogout
	if setting.MailboxServer.MaxMessageSize > 0 && setting.MailboxServer.MaxMessageSize <= int64(^uint32(0)) {
		s.MaxLiteralSize = uint32(setting.MailboxServer.MaxMessageSize)
	}
	return s
}

func serveIMAP(ctx context.Context, server *imapserver.Server, ln net.Listener) {
	go func() {
		<-ctx.Done()
		_ = server.Close()
		_ = ln.Close()
	}()
	if err := server.Serve(ln); err != nil && ctx.Err() == nil {
		log.Error("Mailbox IMAP server stopped: %v", err)
	}
}

func (b *imapBackend) Login(_ *imap.ConnInfo, username, password string) (backend.User, error) {
	user, err := Authenticate(b.ctx, username, password)
	if err != nil {
		return nil, backend.ErrInvalidCredentials
	}
	if err := mailbox_model.EnsureSystemFolders(b.ctx, user.ID); err != nil {
		return nil, err
	}
	return &imapUser{ctx: b.ctx, user: user}, nil
}

func (u *imapUser) Username() string {
	return u.user.Name
}

func (u *imapUser) ListMailboxes(subscribed bool) ([]backend.Mailbox, error) {
	folders, err := mailbox_model.ListFolders(u.ctx, u.user.ID, subscribed)
	if err != nil {
		return nil, err
	}
	mailboxes := make([]backend.Mailbox, 0, len(folders))
	for _, folder := range folders {
		mailboxes = append(mailboxes, &imapMailbox{ctx: u.ctx, user: u.user, name: folder.Name})
	}
	return mailboxes, nil
}

func (u *imapUser) GetMailbox(name string) (backend.Mailbox, error) {
	folder, err := mailbox_model.GetFolder(u.ctx, u.user.ID, mailbox_model.NormalizeFolder(name))
	if err != nil {
		return nil, backend.ErrNoSuchMailbox
	}
	return &imapMailbox{ctx: u.ctx, user: u.user, name: folder.Name}, nil
}

func (u *imapUser) CreateMailbox(name string) error {
	if exists, err := mailbox_model.FolderExists(u.ctx, u.user.ID, name); err != nil {
		return err
	} else if exists {
		return backend.ErrMailboxAlreadyExists
	}
	return mailbox_model.CreateFolder(u.ctx, u.user.ID, name)
}

func (u *imapUser) DeleteMailbox(name string) error {
	exists, err := mailbox_model.FolderExists(u.ctx, u.user.ID, name)
	if err != nil {
		return err
	}
	if !exists {
		return backend.ErrNoSuchMailbox
	}
	return mailbox_model.DeleteFolder(u.ctx, u.user.ID, name)
}

func (u *imapUser) RenameMailbox(existingName, newName string) error {
	existingName = mailbox_model.NormalizeFolder(existingName)
	newName = mailbox_model.NormalizeFolder(newName)
	if exists, err := mailbox_model.FolderExists(u.ctx, u.user.ID, existingName); err != nil {
		return err
	} else if !exists {
		return backend.ErrNoSuchMailbox
	}
	if exists, err := mailbox_model.FolderExists(u.ctx, u.user.ID, newName); err != nil {
		return err
	} else if exists {
		return backend.ErrMailboxAlreadyExists
	}
	if existingName == mailbox_model.FolderInbox {
		if err := mailbox_model.CreateFolder(u.ctx, u.user.ID, newName); err != nil {
			return err
		}
		msgs, err := mailbox_model.ListFolderMessages(u.ctx, u.user.ID, mailbox_model.FolderInbox)
		if err != nil {
			return err
		}
		for _, msg := range msgs {
			if _, err := mailbox_model.CopyMessage(u.ctx, u.user.ID, msg.ID, newName); err != nil {
				return err
			}
			if err := mailbox_model.PurgeMessage(u.ctx, u.user.ID, msg.ID); err != nil {
				return err
			}
		}
		return nil
	}
	return mailbox_model.RenameFolder(u.ctx, u.user.ID, existingName, newName)
}

func (u *imapUser) Logout() error { return nil }

func (m *imapMailbox) Name() string { return m.name }

func (m *imapMailbox) Info() (*imap.MailboxInfo, error) {
	return &imap.MailboxInfo{Delimiter: imapDelimiter, Name: m.name}, nil
}

func (m *imapMailbox) Status(items []imap.StatusItem) (*imap.MailboxStatus, error) {
	folder, err := mailbox_model.GetFolder(m.ctx, m.user.ID, m.name)
	if err != nil {
		return nil, backend.ErrNoSuchMailbox
	}
	msgs, err := mailbox_model.ListFolderMessages(m.ctx, m.user.ID, m.name)
	if err != nil {
		return nil, err
	}
	status := imap.NewMailboxStatus(m.name, items)
	status.Flags = supportedIMAPFlags()
	status.PermanentFlags = []string{imap.SeenFlag, imap.AnsweredFlag, imap.FlaggedFlag, imap.DeletedFlag, imap.DraftFlag}
	status.UidValidity = folder.UIDValidity
	for i, msg := range msgs {
		if !msg.Seen {
			status.Unseen++
			if status.UnseenSeqNum == 0 {
				status.UnseenSeqNum = uint32(i + 1)
			}
		}
		if msg.Recent {
			status.Recent++
		}
	}
	status.UidNext = folder.UIDNext
	status.Messages = uint32(len(msgs))
	return status, nil
}

func supportedIMAPFlags() []string {
	return []string{imap.SeenFlag, imap.AnsweredFlag, imap.FlaggedFlag, imap.DeletedFlag, imap.DraftFlag, imap.RecentFlag}
}

func (m *imapMailbox) SetSubscribed(subscribed bool) error {
	return mailbox_model.SetFolderSubscribed(m.ctx, m.user.ID, m.name, subscribed)
}

func (m *imapMailbox) Check() error { return nil }

func (m *imapMailbox) ListMessages(uid bool, seqSet *imap.SeqSet, items []imap.FetchItem, ch chan<- *imap.Message) error {
	defer close(ch)
	msgs, err := mailbox_model.ListFolderMessages(m.ctx, m.user.ID, m.name)
	if err != nil {
		return err
	}
	for i, stored := range msgs {
		seqNum := uint32(i + 1)
		id := seqNum
		if uid {
			id = stored.UID
		}
		if seqSet != nil && !seqSet.Contains(id) {
			continue
		}
		fetched, markSeen, err := fetchIMAPMessage(stored, seqNum, items, m.rawLoader(stored.ID))
		if err != nil {
			return err
		}
		if markSeen && !stored.Seen {
			if err := mailbox_model.MarkRead(m.ctx, m.user.ID, stored.ID, true); err != nil {
				return err
			}
			stored.Seen = true
			fetched.Flags = messageFlags(stored)
		}
		ch <- fetched
	}
	return nil
}

// rawLoader returns a loader that reads one message body at most once per FETCH,
// so flag-only and UID-only fetches never touch the raw blob at all.
func (m *imapMailbox) rawLoader(messageID int64) func() ([]byte, error) {
	var (
		raw    []byte
		err    error
		loaded bool
	)
	return func() ([]byte, error) {
		if !loaded {
			raw, err = mailbox_model.GetMessageRaw(m.ctx, m.user.ID, messageID)
			loaded = true
		}
		return raw, err
	}
}

func fetchIMAPMessage(stored *mailbox_model.Message, seqNum uint32, items []imap.FetchItem, loadRaw func() ([]byte, error)) (*imap.Message, bool, error) {
	fetched := imap.NewMessage(seqNum, items)
	markSeen := false
	parse := func() (textproto.Header, io.Reader, error) {
		raw, err := loadRaw()
		if err != nil {
			return textproto.Header{}, nil, err
		}
		return headerAndBody(raw)
	}
	for _, item := range items {
		switch item {
		case imap.FetchEnvelope:
			hdr, _, err := parse()
			if err != nil {
				return nil, false, err
			}
			fetched.Envelope, _ = backendutil.FetchEnvelope(hdr)
		case imap.FetchBody, imap.FetchBodyStructure:
			hdr, body, err := parse()
			if err != nil {
				return nil, false, err
			}
			fetched.BodyStructure, _ = backendutil.FetchBodyStructure(hdr, body, item == imap.FetchBodyStructure)
		case imap.FetchFlags:
			fetched.Flags = messageFlags(stored)
		case imap.FetchInternalDate:
			fetched.InternalDate = stored.ReceivedUnix.AsTime()
		case imap.FetchRFC822Size:
			fetched.Size = uint32(min(stored.Size, int64(^uint32(0))))
		case imap.FetchUid:
			fetched.Uid = stored.UID
		default:
			section, err := imap.ParseBodySectionName(item)
			if err != nil {
				continue
			}
			hdr, body, err := parse()
			if err != nil {
				return nil, false, err
			}
			literal, err := backendutil.FetchBodySection(hdr, body, section)
			if err != nil {
				return nil, false, err
			}
			fetched.Body[section] = literal
			if !section.Peek {
				markSeen = true
			}
		}
	}
	return fetched, markSeen, nil
}

func headerAndBody(raw []byte) (textproto.Header, io.Reader, error) {
	body := bufio.NewReader(bytes.NewReader(raw))
	hdr, err := textproto.ReadHeader(body)
	return hdr, body, err
}

func (m *imapMailbox) SearchMessages(uid bool, criteria *imap.SearchCriteria) ([]uint32, error) {
	msgs, err := mailbox_model.ListFolderMessages(m.ctx, m.user.ID, m.name)
	if err != nil {
		return nil, err
	}
	ids := make([]uint32, 0)
	for i, stored := range msgs {
		raw, err := mailbox_model.GetMessageRaw(m.ctx, m.user.ID, stored.ID)
		if err != nil {
			continue
		}
		entity, err := message.Read(bytes.NewReader(raw))
		if err != nil {
			continue
		}
		seqNum := uint32(i + 1)
		matched, err := backendutil.Match(entity, seqNum, stored.UID, stored.ReceivedUnix.AsTime(), messageFlags(stored), criteria)
		if err != nil || !matched {
			continue
		}
		if uid {
			ids = append(ids, stored.UID)
		} else {
			ids = append(ids, seqNum)
		}
	}
	return ids, nil
}

func (m *imapMailbox) CreateMessage(flags []string, date time.Time, body imap.Literal) error {
	raw, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	stored, err := StoreRaw(m.ctx, m.user, m.name, raw, hasFlag(flags, imap.SeenFlag))
	if err != nil {
		return err
	}
	if date.IsZero() {
		date = time.Now()
	}
	if err := mailbox_model.SetMessageInternalDate(m.ctx, m.user.ID, stored.ID, timeutil.TimeStamp(date.Unix())); err != nil {
		return err
	}
	if err := mailbox_model.SetMessageRecent(m.ctx, m.user.ID, stored.ID, true); err != nil {
		return err
	}
	return mailbox_model.SetMessageIMAPFlags(m.ctx, m.user.ID, stored.ID,
		hasFlag(flags, imap.SeenFlag), hasFlag(flags, imap.FlaggedFlag), hasFlag(flags, imap.AnsweredFlag),
		hasFlag(flags, imap.DraftFlag), hasFlag(flags, imap.DeletedFlag))
}

func (m *imapMailbox) UpdateMessagesFlags(uid bool, seqSet *imap.SeqSet, op imap.FlagsOp, flags []string) error {
	msgs, err := mailbox_model.ListFolderMessages(m.ctx, m.user.ID, m.name)
	if err != nil {
		return err
	}
	for i, stored := range msgs {
		id := uint32(i + 1)
		if uid {
			id = stored.UID
		}
		if seqSet != nil && !seqSet.Contains(id) {
			continue
		}
		updated := backendutil.UpdateFlags(messageFlags(stored), op, flags)
		if err := mailbox_model.SetMessageIMAPFlags(m.ctx, m.user.ID, stored.ID,
			hasFlag(updated, imap.SeenFlag), hasFlag(updated, imap.FlaggedFlag), hasFlag(updated, imap.AnsweredFlag),
			hasFlag(updated, imap.DraftFlag), hasFlag(updated, imap.DeletedFlag)); err != nil {
			return err
		}
	}
	return nil
}

func (m *imapMailbox) CopyMessages(uid bool, seqSet *imap.SeqSet, destName string) error {
	if exists, err := mailbox_model.FolderExists(m.ctx, m.user.ID, destName); err != nil {
		return err
	} else if !exists {
		return backend.ErrNoSuchMailbox
	}
	msgs, err := mailbox_model.ListFolderMessages(m.ctx, m.user.ID, m.name)
	if err != nil {
		return err
	}
	for i, stored := range msgs {
		id := uint32(i + 1)
		if uid {
			id = stored.UID
		}
		if seqSet != nil && !seqSet.Contains(id) {
			continue
		}
		if _, err := mailbox_model.CopyMessage(m.ctx, m.user.ID, stored.ID, destName); err != nil {
			return err
		}
	}
	return nil
}

func (m *imapMailbox) Expunge() error {
	return mailbox_model.Expunge(m.ctx, m.user.ID, m.name)
}

func messageFlags(msg *mailbox_model.Message) []string {
	flags := make([]string, 0, 6)
	if msg.Seen {
		flags = append(flags, imap.SeenFlag)
	}
	if msg.Answered {
		flags = append(flags, imap.AnsweredFlag)
	}
	if msg.Flagged {
		flags = append(flags, imap.FlaggedFlag)
	}
	if msg.Deleted {
		flags = append(flags, imap.DeletedFlag)
	}
	if msg.Draft {
		flags = append(flags, imap.DraftFlag)
	}
	if msg.Recent {
		flags = append(flags, imap.RecentFlag)
	}
	return flags
}

func hasFlag(flags []string, wanted string) bool {
	for _, flag := range flags {
		if strings.EqualFold(flag, wanted) {
			return true
		}
	}
	return false
}
