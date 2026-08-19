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
	"sort"
	"strings"
	"sync"

	mailbox_model "gitea.dev/models/mailbox"
	user_model "gitea.dev/models/user"
	"gitea.dev/modules/log"
	"gitea.dev/modules/setting"
	"gitea.dev/modules/timeutil"

	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapserver"
	"github.com/emersion/go-message/textproto"
)

const imapDelimiter = '/'

// mailboxKey identifies one folder of one account, which is the unit a
// MailboxTracker follows.
type mailboxKey struct {
	userID int64
	folder string
}

// trackers keeps one MailboxTracker per folder that has a session on it, so a
// delivery can tell every connected client about new mail. Entries are dropped
// when the last session using one goes away.
var trackers = struct {
	sync.Mutex
	m map[mailboxKey]*trackedMailbox
}{m: make(map[mailboxKey]*trackedMailbox)}

type trackedMailbox struct {
	tracker  *imapserver.MailboxTracker
	refCount int
}

func acquireTracker(key mailboxKey, numMessages uint32) *imapserver.MailboxTracker {
	trackers.Lock()
	defer trackers.Unlock()
	entry, ok := trackers.m[key]
	if !ok {
		entry = &trackedMailbox{tracker: imapserver.NewMailboxTracker(numMessages)}
		trackers.m[key] = entry
	}
	entry.refCount++
	return entry.tracker
}

func releaseTracker(key mailboxKey) {
	trackers.Lock()
	defer trackers.Unlock()
	entry, ok := trackers.m[key]
	if !ok {
		return
	}
	entry.refCount--
	if entry.refCount <= 0 {
		delete(trackers.m, key)
	}
}

// lookupTracker returns the tracker for a folder only when something is
// watching it; there is nobody to notify otherwise.
func lookupTracker(key mailboxKey) *imapserver.MailboxTracker {
	trackers.Lock()
	defer trackers.Unlock()
	if entry, ok := trackers.m[key]; ok {
		return entry.tracker
	}
	return nil
}

// NotifyMailboxUpdate tells sessions watching a folder that its contents
// changed, so a client in IDLE hears about mail as it is delivered rather than
// at its next poll.
func NotifyMailboxUpdate(ctx context.Context, user *user_model.User, folder string) {
	if user == nil {
		return
	}
	folder = mailbox_model.NormalizeFolder(folder)
	tracker := lookupTracker(mailboxKey{userID: user.ID, folder: folder})
	if tracker == nil {
		return
	}
	msgs, err := mailbox_model.ListFolderMessages(ctx, user.ID, folder)
	if err != nil {
		log.Debug("Mailbox IMAP: cannot count %q for user %d: %v", folder, user.ID, err)
		return
	}
	tracker.QueueNumMessages(uint32(len(msgs)))
}

func initIMAP(ctx context.Context, tlsConfig *tls.Config) error {
	options := &imapserver.Options{
		NewSession: func(conn *imapserver.Conn) (imapserver.Session, *imapserver.GreetingData, error) {
			return &imapSession{ctx: ctx}, nil, nil
		},
		Caps: imap.CapSet{
			imap.CapIMAP4rev1:    {},
			imap.CapIMAP4rev2:    {},
			imap.CapNamespace:    {},
			imap.CapUIDPlus:      {},
			imap.CapESearch:      {},
			imap.CapListExtended: {},
			imap.CapListStatus:   {},
			imap.CapMove:         {},
			imap.CapStatusSize:   {},
		},
		InsecureAuth: setting.MailboxServer.AllowInsecureAuth,
		Logger:       imapLogger{},
	}

	if addr := strings.TrimSpace(setting.MailboxServer.IMAPListen); addr != "" {
		options := *options
		options.TLSConfig = tlsConfig
		server := imapserver.New(&options)
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
		server := imapserver.New(options)
		ln, err := tls.Listen("tcp", addr, tlsConfig.Clone())
		if err != nil {
			return fmt.Errorf("listen on IMAPS %s: %w", addr, err)
		}
		log.Info("Mailbox IMAPS listening on %s", addr)
		go serveIMAP(ctx, server, ln)
	}
	return nil
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

type imapLogger struct{}

func (imapLogger) Printf(format string, v ...any) {
	log.Warn("Mailbox IMAP: "+format, v...)
}

// imapSession is one client connection. The selected-state fields are only set
// between SELECT and UNSELECT.
type imapSession struct {
	ctx  context.Context
	user *user_model.User

	folder      string
	tracker     *imapserver.SessionTracker
	numMessages uint32
}

var (
	_ imapserver.Session          = (*imapSession)(nil)
	_ imapserver.SessionNamespace = (*imapSession)(nil)
	_ imapserver.SessionMove      = (*imapSession)(nil)
	_ imapserver.SessionIMAP4rev2 = (*imapSession)(nil)
)

func imapNo(format string, args ...any) error {
	return &imap.Error{Type: imap.StatusResponseTypeNo, Text: fmt.Sprintf(format, args...)}
}

func (s *imapSession) Close() error {
	s.closeMailbox()
	return nil
}

func (s *imapSession) closeMailbox() {
	if s.tracker != nil {
		s.tracker.Close()
		s.tracker = nil
		releaseTracker(mailboxKey{userID: s.user.ID, folder: s.folder})
	}
	s.folder = ""
	s.numMessages = 0
}

func (s *imapSession) Login(username, password string) error {
	user, err := Authenticate(s.ctx, username, password)
	if err != nil {
		return imapserver.ErrAuthFailed
	}
	if err := mailbox_model.EnsureSystemFolders(s.ctx, user.ID); err != nil {
		return err
	}
	s.user = user
	return nil
}

func (s *imapSession) Namespace() (*imap.NamespaceData, error) {
	return &imap.NamespaceData{
		Personal: []imap.NamespaceDescriptor{{Delim: imapDelimiter}},
	}, nil
}

func (s *imapSession) Select(name string, _ *imap.SelectOptions) (*imap.SelectData, error) {
	folder := mailbox_model.NormalizeFolder(name)
	dbFolder, err := mailbox_model.GetFolder(s.ctx, s.user.ID, folder)
	if err != nil {
		return nil, imapNo("no such mailbox")
	}
	msgs, err := mailbox_model.ListFolderMessages(s.ctx, s.user.ID, folder)
	if err != nil {
		return nil, err
	}

	s.closeMailbox()
	s.folder = folder
	s.numMessages = uint32(len(msgs))
	s.tracker = acquireTracker(mailboxKey{userID: s.user.ID, folder: folder}, s.numMessages).NewSession()

	flags := supportedIMAPFlags()
	permanent := append(append([]imap.Flag(nil), flags...), imap.FlagWildcard)
	data := &imap.SelectData{
		Flags:          flags,
		PermanentFlags: permanent,
		NumMessages:    s.numMessages,
		UIDNext:        imap.UID(dbFolder.UIDNext),
		UIDValidity:    dbFolder.UIDValidity,
	}
	for i, msg := range msgs {
		if !msg.Seen {
			data.FirstUnseenSeqNum = uint32(i + 1)
			break
		}
	}
	return data, nil
}

func (s *imapSession) Unselect() error {
	s.closeMailbox()
	return nil
}

func supportedIMAPFlags() []imap.Flag {
	return []imap.Flag{imap.FlagSeen, imap.FlagAnswered, imap.FlagFlagged, imap.FlagDeleted, imap.FlagDraft}
}

func (s *imapSession) Create(name string, _ *imap.CreateOptions) error {
	if err := mailbox_model.CreateFolder(s.ctx, s.user.ID, name); err != nil {
		return &imap.Error{Type: imap.StatusResponseTypeNo, Code: imap.ResponseCodeAlreadyExists, Text: err.Error()}
	}
	return nil
}

func (s *imapSession) Delete(name string) error {
	if err := mailbox_model.DeleteFolder(s.ctx, s.user.ID, name); err != nil {
		return imapNo("%s", err.Error())
	}
	return nil
}

func (s *imapSession) Rename(name, newName string, _ *imap.RenameOptions) error {
	if err := mailbox_model.RenameFolder(s.ctx, s.user.ID, name, newName); err != nil {
		return imapNo("%s", err.Error())
	}
	return nil
}

func (s *imapSession) Subscribe(name string) error {
	return mailbox_model.SetFolderSubscribed(s.ctx, s.user.ID, name, true)
}

func (s *imapSession) Unsubscribe(name string) error {
	return mailbox_model.SetFolderSubscribed(s.ctx, s.user.ID, name, false)
}

func (s *imapSession) List(w *imapserver.ListWriter, ref string, patterns []string, options *imap.ListOptions) error {
	// An empty pattern is the client asking only for the hierarchy delimiter.
	if len(patterns) == 0 {
		return w.WriteList(&imap.ListData{
			Attrs: []imap.MailboxAttr{imap.MailboxAttrNoSelect},
			Delim: imapDelimiter,
		})
	}

	folders, err := mailbox_model.ListFolders(s.ctx, s.user.ID, false)
	if err != nil {
		return err
	}
	entries := make([]imap.ListData, 0, len(folders))
	for _, folder := range folders {
		matched := false
		for _, pattern := range patterns {
			if imapserver.MatchList(folder.Name, imapDelimiter, ref, pattern) {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		if options.SelectSubscribed && !folder.Subscribed {
			continue
		}

		data := imap.ListData{Mailbox: folder.Name, Delim: imapDelimiter}
		if folder.Subscribed {
			data.Attrs = append(data.Attrs, imap.MailboxAttrSubscribed)
		}
		if attr := specialUseAttr(folder.Name); attr != "" {
			data.Attrs = append(data.Attrs, attr)
		}
		if options.ReturnStatus != nil {
			status, err := s.Status(folder.Name, options.ReturnStatus)
			if err != nil {
				return err
			}
			data.Status = status
		}
		entries = append(entries, data)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Mailbox < entries[j].Mailbox })

	for i := range entries {
		if err := w.WriteList(&entries[i]); err != nil {
			return err
		}
	}
	return nil
}

// specialUseAttr maps our system folders onto RFC 6154 attributes, which is how
// a client knows which folder is Sent, Trash and so on.
func specialUseAttr(name string) imap.MailboxAttr {
	switch mailbox_model.NormalizeFolder(name) {
	case mailbox_model.FolderSent:
		return imap.MailboxAttrSent
	case mailbox_model.FolderDrafts:
		return imap.MailboxAttrDrafts
	case mailbox_model.FolderTrash:
		return imap.MailboxAttrTrash
	case mailbox_model.FolderArchive:
		return imap.MailboxAttrArchive
	case mailbox_model.FolderJunk:
		return imap.MailboxAttrJunk
	default:
		return ""
	}
}

func (s *imapSession) Status(name string, options *imap.StatusOptions) (*imap.StatusData, error) {
	folder := mailbox_model.NormalizeFolder(name)
	dbFolder, err := mailbox_model.GetFolder(s.ctx, s.user.ID, folder)
	if err != nil {
		return nil, imapNo("no such mailbox")
	}
	msgs, err := mailbox_model.ListFolderMessages(s.ctx, s.user.ID, folder)
	if err != nil {
		return nil, err
	}

	data := &imap.StatusData{Mailbox: folder}
	if options.NumMessages {
		num := uint32(len(msgs))
		data.NumMessages = &num
	}
	if options.UIDNext {
		data.UIDNext = imap.UID(dbFolder.UIDNext)
	}
	if options.UIDValidity {
		data.UIDValidity = dbFolder.UIDValidity
	}
	if options.NumUnseen {
		var num uint32
		for _, msg := range msgs {
			if !msg.Seen {
				num++
			}
		}
		data.NumUnseen = &num
	}
	if options.NumDeleted {
		var num uint32
		for _, msg := range msgs {
			if msg.Deleted {
				num++
			}
		}
		data.NumDeleted = &num
	}
	if options.Size {
		var size int64
		for _, msg := range msgs {
			size += msg.Size
		}
		data.Size = &size
	}
	if options.NumRecent {
		var num uint32
		for _, msg := range msgs {
			if msg.Recent {
				num++
			}
		}
		data.NumRecent = &num
	}
	return data, nil
}

func (s *imapSession) Append(mailbox string, r imap.LiteralReader, options *imap.AppendOptions) (*imap.AppendData, error) {
	folder := mailbox_model.NormalizeFolder(mailbox)
	if exists, err := mailbox_model.FolderExists(s.ctx, s.user.ID, folder); err != nil {
		return nil, err
	} else if !exists {
		return nil, &imap.Error{Type: imap.StatusResponseTypeNo, Code: imap.ResponseCodeTryCreate, Text: "no such mailbox"}
	}
	raw, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	seen := false
	for _, flag := range options.Flags {
		if flag == imap.FlagSeen {
			seen = true
		}
	}
	stored, err := StoreRaw(s.ctx, s.user, folder, raw, seen)
	if err != nil {
		return nil, err
	}
	if !options.Time.IsZero() {
		if err := mailbox_model.SetMessageInternalDate(s.ctx, s.user.ID, stored.ID, timeutil.TimeStamp(options.Time.Unix())); err != nil {
			return nil, err
		}
	}
	if err := applyIMAPFlags(s.ctx, s.user.ID, stored.ID, options.Flags); err != nil {
		return nil, err
	}

	dbFolder, err := mailbox_model.GetFolder(s.ctx, s.user.ID, folder)
	if err != nil {
		return nil, err
	}
	return &imap.AppendData{UID: imap.UID(stored.UID), UIDValidity: dbFolder.UIDValidity}, nil
}

func (s *imapSession) Poll(w *imapserver.UpdateWriter, allowExpunge bool) error {
	if s.tracker == nil {
		return nil
	}
	return s.tracker.Poll(w, allowExpunge)
}

func (s *imapSession) Idle(w *imapserver.UpdateWriter, stop <-chan struct{}) error {
	if s.tracker == nil {
		<-stop
		return nil
	}
	return s.tracker.Idle(w, stop)
}

// messages returns the current folder contents in UID order, which is the
// order sequence numbers are assigned in.
func (s *imapSession) messages() ([]*mailbox_model.Message, error) {
	if s.folder == "" {
		return nil, imapNo("no mailbox selected")
	}
	return mailbox_model.ListFolderMessages(s.ctx, s.user.ID, s.folder)
}

// forEach walks the messages a command addresses, resolving the sequence or UID
// set against the session's current view.
func (s *imapSession) forEach(numSet imap.NumSet, fn func(seqNum uint32, msg *mailbox_model.Message) error) error {
	msgs, err := s.messages()
	if err != nil {
		return err
	}
	numSet = s.staticNumSet(numSet, msgs)
	for i, msg := range msgs {
		seqNum := uint32(i + 1)
		var contains bool
		switch set := numSet.(type) {
		case imap.SeqSet:
			clientSeq := s.tracker.EncodeSeqNum(seqNum)
			contains = clientSeq != 0 && set.Contains(clientSeq)
		case imap.UIDSet:
			contains = set.Contains(imap.UID(msg.UID))
		}
		if !contains {
			continue
		}
		if err := fn(seqNum, msg); err != nil {
			return err
		}
	}
	return nil
}

// staticNumSet resolves "*" against the current mailbox, so a range that ends
// at the last message means the same thing to us as to the client.
func (s *imapSession) staticNumSet(numSet imap.NumSet, msgs []*mailbox_model.Message) imap.NumSet {
	var maxUID imap.UID
	if len(msgs) > 0 {
		maxUID = imap.UID(msgs[len(msgs)-1].UID)
	}
	switch set := numSet.(type) {
	case imap.SeqSet:
		maxSeq := uint32(len(msgs))
		for i := range set {
			staticNumRange(&set[i].Start, &set[i].Stop, maxSeq)
		}
		return set
	case imap.UIDSet:
		for i := range set {
			staticNumRange((*uint32)(&set[i].Start), (*uint32)(&set[i].Stop), uint32(maxUID))
		}
		return set
	}
	return numSet
}

func staticNumRange(start, stop *uint32, maxNum uint32) {
	dynamic := false
	if *start == 0 {
		*start = maxNum
		dynamic = true
	}
	if *stop == 0 {
		*stop = maxNum
		dynamic = true
	}
	if dynamic && *start > *stop {
		*start, *stop = *stop, *start
	}
}

func (s *imapSession) Fetch(w *imapserver.FetchWriter, numSet imap.NumSet, options *imap.FetchOptions) error {
	markSeen := false
	for _, section := range options.BodySection {
		if !section.Peek {
			markSeen = true
			break
		}
	}

	return s.forEach(numSet, func(seqNum uint32, msg *mailbox_model.Message) error {
		if markSeen && !msg.Seen {
			if err := mailbox_model.MarkRead(s.ctx, s.user.ID, msg.ID, true); err != nil {
				return err
			}
			msg.Seen = true
		}
		clientSeq := s.tracker.EncodeSeqNum(seqNum)
		if clientSeq == 0 {
			return nil
		}
		return s.writeMessage(w.CreateMessage(clientSeq), msg, options)
	})
}

// writeMessage answers one FETCH item set. The raw bytes are read only when a
// requested item actually needs them.
func (s *imapSession) writeMessage(w *imapserver.FetchResponseWriter, msg *mailbox_model.Message, options *imap.FetchOptions) error {
	w.WriteUID(imap.UID(msg.UID))
	if options.Flags {
		w.WriteFlags(messageFlags(msg))
	}
	if options.InternalDate {
		w.WriteInternalDate(msg.ReceivedUnix.AsTime())
	}
	if options.RFC822Size {
		w.WriteRFC822Size(msg.Size)
	}

	needsRaw := options.Envelope || options.BodyStructure != nil ||
		len(options.BodySection) > 0 || len(options.BinarySection) > 0 || len(options.BinarySectionSize) > 0
	var raw []byte
	if needsRaw {
		loaded, err := mailbox_model.GetMessageRaw(s.ctx, s.user.ID, msg.ID)
		if err != nil {
			return err
		}
		raw = loaded
	}

	if options.Envelope {
		header, err := textproto.ReadHeader(bufio.NewReader(bytes.NewReader(raw)))
		if err != nil {
			return err
		}
		w.WriteEnvelope(imapserver.ExtractEnvelope(header))
	}
	if options.BodyStructure != nil {
		w.WriteBodyStructure(imapserver.ExtractBodyStructure(bytes.NewReader(raw)))
	}
	for _, section := range options.BodySection {
		buf := imapserver.ExtractBodySection(bytes.NewReader(raw), section)
		if err := writeSection(w.WriteBodySection(section, int64(len(buf))), buf); err != nil {
			return err
		}
	}
	for _, section := range options.BinarySection {
		buf := imapserver.ExtractBinarySection(bytes.NewReader(raw), section)
		if err := writeSection(w.WriteBinarySection(section, int64(len(buf))), buf); err != nil {
			return err
		}
	}
	for _, section := range options.BinarySectionSize {
		w.WriteBinarySectionSize(section, imapserver.ExtractBinarySectionSize(bytes.NewReader(raw), section))
	}
	return w.Close()
}

func writeSection(wc io.WriteCloser, buf []byte) error {
	_, writeErr := wc.Write(buf)
	closeErr := wc.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

func (s *imapSession) Store(w *imapserver.FetchWriter, numSet imap.NumSet, flags *imap.StoreFlags, options *imap.StoreOptions) error {
	err := s.forEach(numSet, func(seqNum uint32, msg *mailbox_model.Message) error {
		updated := applyStore(messageFlags(msg), flags)
		if err := storeMessageFlags(s.ctx, s.user.ID, msg.ID, updated); err != nil {
			return err
		}
		if tracker := lookupTracker(mailboxKey{userID: s.user.ID, folder: s.folder}); tracker != nil {
			tracker.QueueMessageFlags(seqNum, imap.UID(msg.UID), updated, s.tracker)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if flags.Silent {
		return nil
	}
	return s.Fetch(w, numSet, &imap.FetchOptions{Flags: true})
}

// applyStore resolves an ADD/REMOVE/SET flag operation onto a message's flags.
func applyStore(current []imap.Flag, store *imap.StoreFlags) []imap.Flag {
	set := make(map[imap.Flag]struct{}, len(current))
	if store.Op != imap.StoreFlagsSet {
		for _, flag := range current {
			set[flag] = struct{}{}
		}
	}
	for _, flag := range store.Flags {
		if store.Op == imap.StoreFlagsDel {
			delete(set, flag)
		} else {
			set[flag] = struct{}{}
		}
	}
	out := make([]imap.Flag, 0, len(set))
	for _, flag := range supportedIMAPFlags() {
		if _, ok := set[flag]; ok {
			out = append(out, flag)
		}
	}
	return out
}

func storeMessageFlags(ctx context.Context, userID, messageID int64, flags []imap.Flag) error {
	has := func(want imap.Flag) bool {
		for _, flag := range flags {
			if flag == want {
				return true
			}
		}
		return false
	}
	return mailbox_model.SetMessageIMAPFlags(ctx, userID, messageID,
		has(imap.FlagSeen), has(imap.FlagFlagged), has(imap.FlagAnswered), has(imap.FlagDraft), has(imap.FlagDeleted))
}

func applyIMAPFlags(ctx context.Context, userID, messageID int64, flags []imap.Flag) error {
	return storeMessageFlags(ctx, userID, messageID, flags)
}

func messageFlags(msg *mailbox_model.Message) []imap.Flag {
	flags := make([]imap.Flag, 0, 6)
	if msg.Seen {
		flags = append(flags, imap.FlagSeen)
	}
	if msg.Answered {
		flags = append(flags, imap.FlagAnswered)
	}
	if msg.Flagged {
		flags = append(flags, imap.FlagFlagged)
	}
	if msg.Deleted {
		flags = append(flags, imap.FlagDeleted)
	}
	if msg.Draft {
		flags = append(flags, imap.FlagDraft)
	}
	return flags
}

func (s *imapSession) Copy(numSet imap.NumSet, dest string) (*imap.CopyData, error) {
	folder := mailbox_model.NormalizeFolder(dest)
	if exists, err := mailbox_model.FolderExists(s.ctx, s.user.ID, folder); err != nil {
		return nil, err
	} else if !exists {
		return nil, &imap.Error{Type: imap.StatusResponseTypeNo, Code: imap.ResponseCodeTryCreate, Text: "no such mailbox"}
	}

	var sourceUIDs, destUIDs imap.UIDSet
	err := s.forEach(numSet, func(_ uint32, msg *mailbox_model.Message) error {
		copied, err := mailbox_model.CopyMessage(s.ctx, s.user.ID, msg.ID, folder)
		if err != nil {
			return err
		}
		sourceUIDs.AddNum(imap.UID(msg.UID))
		destUIDs.AddNum(imap.UID(copied.UID))
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(sourceUIDs) == 0 {
		return nil, nil
	}

	dbFolder, err := mailbox_model.GetFolder(s.ctx, s.user.ID, folder)
	if err != nil {
		return nil, err
	}
	NotifyMailboxUpdate(s.ctx, s.user, folder)
	return &imap.CopyData{UIDValidity: dbFolder.UIDValidity, SourceUIDs: sourceUIDs, DestUIDs: destUIDs}, nil
}

func (s *imapSession) Move(w *imapserver.MoveWriter, numSet imap.NumSet, dest string) error {
	folder := mailbox_model.NormalizeFolder(dest)
	if exists, err := mailbox_model.FolderExists(s.ctx, s.user.ID, folder); err != nil {
		return err
	} else if !exists {
		return &imap.Error{Type: imap.StatusResponseTypeNo, Code: imap.ResponseCodeTryCreate, Text: "no such mailbox"}
	}

	type moved struct {
		seqNum   uint32
		sourceID imap.UID
	}
	var (
		sourceUIDs, destUIDs imap.UIDSet
		movedMessages        []moved
	)
	err := s.forEach(numSet, func(seqNum uint32, msg *mailbox_model.Message) error {
		sourceUID := imap.UID(msg.UID)
		if err := mailbox_model.MoveMessage(s.ctx, s.user.ID, msg.ID, folder); err != nil {
			return err
		}
		reloaded, err := mailbox_model.GetMessage(s.ctx, s.user.ID, msg.ID)
		if err != nil {
			return err
		}
		sourceUIDs.AddNum(sourceUID)
		destUIDs.AddNum(imap.UID(reloaded.UID))
		movedMessages = append(movedMessages, moved{seqNum: seqNum, sourceID: sourceUID})
		return nil
	})
	if err != nil {
		return err
	}
	if len(movedMessages) == 0 {
		return nil
	}

	dbFolder, err := mailbox_model.GetFolder(s.ctx, s.user.ID, folder)
	if err != nil {
		return err
	}
	if err := w.WriteCopyData(&imap.CopyData{UIDValidity: dbFolder.UIDValidity, SourceUIDs: sourceUIDs, DestUIDs: destUIDs}); err != nil {
		return err
	}

	// Expunge from the highest sequence number down, so each removal does not
	// shift the numbers still to be reported.
	sort.Slice(movedMessages, func(i, j int) bool { return movedMessages[i].seqNum > movedMessages[j].seqNum })
	sourceTracker := lookupTracker(mailboxKey{userID: s.user.ID, folder: s.folder})
	for _, entry := range movedMessages {
		if err := w.WriteExpunge(s.tracker.EncodeSeqNum(entry.seqNum)); err != nil {
			return err
		}
		if sourceTracker != nil {
			sourceTracker.QueueExpunge(entry.seqNum)
		}
	}
	NotifyMailboxUpdate(s.ctx, s.user, folder)
	return nil
}

func (s *imapSession) Expunge(w *imapserver.ExpungeWriter, uids *imap.UIDSet) error {
	msgs, err := s.messages()
	if err != nil {
		return err
	}
	tracker := lookupTracker(mailboxKey{userID: s.user.ID, folder: s.folder})

	// Walk backwards so the sequence numbers of the messages still to be
	// removed stay valid as earlier ones disappear.
	for i := len(msgs) - 1; i >= 0; i-- {
		msg := msgs[i]
		if !msg.Deleted {
			continue
		}
		if uids != nil && !uids.Contains(imap.UID(msg.UID)) {
			continue
		}
		seqNum := uint32(i + 1)
		if err := mailbox_model.PurgeMessage(s.ctx, s.user.ID, msg.ID); err != nil {
			return err
		}
		if clientSeq := s.tracker.EncodeSeqNum(seqNum); clientSeq != 0 {
			if err := w.WriteExpunge(clientSeq); err != nil {
				return err
			}
		}
		if tracker != nil {
			tracker.QueueExpunge(seqNum)
		}
	}
	return nil
}

func (s *imapSession) Search(kind imapserver.NumKind, criteria *imap.SearchCriteria, _ *imap.SearchOptions) (*imap.SearchData, error) {
	msgs, err := s.messages()
	if err != nil {
		return nil, err
	}

	data := &imap.SearchData{}
	var (
		seqNums imap.SeqSet
		uids    imap.UIDSet
		count   uint32
		minSeen bool
	)
	for i, msg := range msgs {
		seqNum := uint32(i + 1)
		if !s.matchSearch(seqNum, msg, criteria) {
			continue
		}
		count++
		uid := imap.UID(msg.UID)
		if kind == imapserver.NumKindUID {
			uids.AddNum(uid)
			data.Max = uint32(uid)
			if !minSeen {
				data.Min, minSeen = uint32(uid), true
			}
		} else {
			clientSeq := s.tracker.EncodeSeqNum(seqNum)
			if clientSeq == 0 {
				continue
			}
			seqNums.AddNum(clientSeq)
			data.Max = clientSeq
			if !minSeen {
				data.Min, minSeen = clientSeq, true
			}
		}
	}
	data.Count = count
	if kind == imapserver.NumKindUID {
		data.All = uids
	} else {
		data.All = seqNums
	}
	return data, nil
}

// matchSearch evaluates the criteria a client can express against the columns
// already extracted at delivery time, so a search does not reparse every
// message body.
func (s *imapSession) matchSearch(seqNum uint32, msg *mailbox_model.Message, criteria *imap.SearchCriteria) bool {
	for _, seqSet := range criteria.SeqNum {
		clientSeq := s.tracker.EncodeSeqNum(seqNum)
		if clientSeq == 0 || !seqSet.Contains(clientSeq) {
			return false
		}
	}
	for _, uidSet := range criteria.UID {
		if !uidSet.Contains(imap.UID(msg.UID)) {
			return false
		}
	}

	if !criteria.Since.IsZero() && msg.ReceivedUnix.AsTime().Before(criteria.Since) {
		return false
	}
	if !criteria.Before.IsZero() && !msg.ReceivedUnix.AsTime().Before(criteria.Before) {
		return false
	}
	sent := msg.SentUnix.AsTime()
	if !criteria.SentSince.IsZero() && sent.Before(criteria.SentSince) {
		return false
	}
	if !criteria.SentBefore.IsZero() && !sent.Before(criteria.SentBefore) {
		return false
	}
	if criteria.Larger > 0 && msg.Size <= criteria.Larger {
		return false
	}
	if criteria.Smaller > 0 && msg.Size >= criteria.Smaller {
		return false
	}

	flags := messageFlags(msg)
	for _, flag := range criteria.Flag {
		if !hasFlag(flags, flag) {
			return false
		}
	}
	for _, flag := range criteria.NotFlag {
		if hasFlag(flags, flag) {
			return false
		}
	}

	for _, header := range criteria.Header {
		if !strings.Contains(strings.ToLower(headerValue(msg, header.Key)), strings.ToLower(header.Value)) {
			return false
		}
	}
	for _, text := range criteria.Body {
		if !strings.Contains(strings.ToLower(msg.TextBody), strings.ToLower(text)) {
			return false
		}
	}
	for _, text := range criteria.Text {
		haystack := strings.ToLower(strings.Join([]string{msg.Subject, msg.FromAddress, msg.FromName, msg.To, msg.Cc, msg.TextBody}, "\n"))
		if !strings.Contains(haystack, strings.ToLower(text)) {
			return false
		}
	}

	for _, not := range criteria.Not {
		if s.matchSearch(seqNum, msg, &not) {
			return false
		}
	}
	for _, or := range criteria.Or {
		if !s.matchSearch(seqNum, msg, &or[0]) && !s.matchSearch(seqNum, msg, &or[1]) {
			return false
		}
	}
	return true
}

func headerValue(msg *mailbox_model.Message, key string) string {
	switch strings.ToLower(key) {
	case "subject":
		return msg.Subject
	case "from":
		return msg.FromName + " <" + msg.FromAddress + ">"
	case "to":
		return msg.To
	case "cc":
		return msg.Cc
	case "bcc":
		return msg.Bcc
	case "reply-to":
		return msg.ReplyTo
	case "message-id":
		return msg.InternetMessageID
	case "in-reply-to":
		return msg.InReplyTo
	case "references":
		return msg.References
	default:
		return ""
	}
}

func hasFlag(flags []imap.Flag, wanted imap.Flag) bool {
	for _, flag := range flags {
		if flag == wanted {
			return true
		}
	}
	return false
}
