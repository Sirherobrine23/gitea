// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package mailbox

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"gitea.dev/models/db"
	"gitea.dev/modules/timeutil"

	"xorm.io/builder"
)

const (
	FolderInbox   = "INBOX"
	FolderSent    = "Sent"
	FolderDrafts  = "Drafts"
	FolderTrash   = "Trash"
	FolderArchive = "Archive"
	FolderJunk    = "Junk"
)

var SystemFolders = []string{FolderInbox, FolderSent, FolderDrafts, FolderTrash, FolderArchive, FolderJunk}

var ErrMessageNotExist = errors.New("mailbox message does not exist")

// Message is one mailbox copy of an RFC 5322 message. Raw contains the canonical
// wire representation used by IMAP and for downloads; the denormalized fields are
// intentionally kept next to it so the web UI and searches do not need to parse MIME.
type MailMessage struct {
	ID     int64  `xorm:"pk autoincr"`
	UserID int64  `xorm:"INDEX UNIQUE(mail_uid) NOT NULL"`
	Folder string `xorm:"INDEX UNIQUE(mail_uid) VARCHAR(255) NOT NULL"`
	UID    uint32 `xorm:"INDEX UNIQUE(mail_uid) NOT NULL"`

	InternetMessageID   string `xorm:"VARCHAR(998)"`
	InternetMessageHash string `xorm:"INDEX VARCHAR(64)"`
	InReplyTo           string `xorm:"VARCHAR(998)"`
	References          string `xorm:"TEXT"`

	FromName    string `xorm:"VARCHAR(255)"`
	FromAddress string `xorm:"INDEX VARCHAR(320)"`
	To          string `xorm:"TEXT"`
	Cc          string `xorm:"TEXT"`
	Bcc         string `xorm:"TEXT"`
	ReplyTo     string `xorm:"TEXT"`
	Subject     string `xorm:"TEXT"`
	TextBody    string `xorm:"LONGTEXT"`
	HTMLBody    string `xorm:"LONGTEXT"`
	Raw         []byte `xorm:"LONGBLOB"`
	Size        int64  `xorm:"NOT NULL"`

	Seen      bool `xorm:"INDEX NOT NULL DEFAULT false"`
	Flagged   bool `xorm:"INDEX NOT NULL DEFAULT false"`
	Answered  bool `xorm:"NOT NULL DEFAULT false"`
	Draft     bool `xorm:"NOT NULL DEFAULT false"`
	Deleted   bool `xorm:"INDEX NOT NULL DEFAULT false"`
	Recent    bool `xorm:"NOT NULL DEFAULT true"`
	HasAttach bool `xorm:"INDEX NOT NULL DEFAULT false"`

	SentUnix     timeutil.TimeStamp `xorm:"INDEX"`
	ReceivedUnix timeutil.TimeStamp `xorm:"INDEX NOT NULL"`
	CreatedUnix  timeutil.TimeStamp `xorm:"created NOT NULL"`
	UpdatedUnix  timeutil.TimeStamp `xorm:"updated NOT NULL"`
}

// Attachment stores a MIME attachment for web download. The same bytes are also
// present in Message.Raw; keeping an extracted copy makes authenticated downloads
// cheap and avoids reparsing untrusted MIME on every request.
type MailAttachment struct {
	ID        int64 `xorm:"pk autoincr"`
	MessageID int64 `xorm:"INDEX NOT NULL"`
	UserID    int64 `xorm:"INDEX NOT NULL"`

	Filename    string             `xorm:"VARCHAR(1024)"`
	ContentType string             `xorm:"VARCHAR(255)"`
	ContentID   string             `xorm:"VARCHAR(998)"`
	Disposition string             `xorm:"VARCHAR(64)"`
	Size        int64              `xorm:"NOT NULL"`
	Content     []byte             `xorm:"LONGBLOB"`
	CreatedUnix timeutil.TimeStamp `xorm:"created NOT NULL"`
}

// Folder records user-created folders. System folders are virtual and always exist.
type MailFolder struct {
	ID          int64              `xorm:"pk autoincr"`
	UserID      int64              `xorm:"INDEX UNIQUE(s) NOT NULL"`
	Name        string             `xorm:"UNIQUE(s) VARCHAR(255) NOT NULL"`
	Subscribed  bool               `xorm:"NOT NULL DEFAULT true"`
	UIDValidity uint32             `xorm:"NOT NULL DEFAULT 1"`
	UIDNext     uint32             `xorm:"NOT NULL DEFAULT 1"`
	CreatedUnix timeutil.TimeStamp `xorm:"created NOT NULL"`
	UpdatedUnix timeutil.TimeStamp `xorm:"updated NOT NULL"`
}

// Alias kinds. Both deliver to the owning account; the kind records where the
// binding came from and drives what an administrator may do with it.
const (
	// AliasKindManual is an address an administrator assigned to an account.
	AliasKindManual = "manual"
	// AliasKindRetired is the local-part an account used before it was renamed.
	// It keeps that mail identity bound to the original owner, so a new account
	// that later claims the freed username cannot receive their mail.
	AliasKindRetired = "retired"
)

// Alias binds a local-part to a Gitea account. This table is authoritative for
// address ownership: a username only yields an address when no alias claims it.
type MailAlias struct {
	ID          int64              `xorm:"pk autoincr"`
	UserID      int64              `xorm:"INDEX NOT NULL"`
	LocalPart   string             `xorm:"UNIQUE VARCHAR(255) NOT NULL"`
	Kind        string             `xorm:"VARCHAR(16) NOT NULL DEFAULT 'manual'"`
	CreatedUnix timeutil.TimeStamp `xorm:"created NOT NULL"`
}

// Short aliases keep the mailbox package API readable while the canonical type
// names stay globally unique for db.NamesToBean, which also indexes models by
// reflected Go type name.
type (
	Message    = MailMessage
	Attachment = MailAttachment
	Folder     = MailFolder
	Alias      = MailAlias
)

func (*MailMessage) TableName() string    { return "mailbox_message" }
func (*MailAttachment) TableName() string { return "mailbox_attachment" }
func (*MailFolder) TableName() string     { return "mailbox_folder" }
func (*MailAlias) TableName() string      { return "mailbox_alias" }

func init() {
	db.RegisterModel(new(MailMessage))
	db.RegisterModel(new(MailAttachment))
	db.RegisterModel(new(MailFolder))
	db.RegisterModel(new(MailAlias))
}

func newUIDValidity() uint32 {
	var b [4]byte
	if _, err := rand.Read(b[:]); err == nil {
		if v := binary.BigEndian.Uint32(b[:]); v != 0 {
			return v
		}
	}
	v := uint32(time.Now().UnixNano())
	if v == 0 {
		return 1
	}
	return v
}

func newFolder(userID int64, name string) *Folder {
	return &Folder{UserID: userID, Name: name, Subscribed: true, UIDValidity: newUIDValidity(), UIDNext: 1}
}

func NormalizeFolder(name string) string {
	name = strings.TrimSpace(name)
	if strings.EqualFold(name, FolderInbox) {
		return FolderInbox
	}
	for _, system := range SystemFolders[1:] {
		if strings.EqualFold(name, system) {
			return system
		}
	}
	return name
}

func IsSystemFolder(name string) bool {
	name = NormalizeFolder(name)
	for _, folder := range SystemFolders {
		if name == folder {
			return true
		}
	}
	return false
}

func FolderExists(ctx context.Context, userID int64, name string) (bool, error) {
	name = NormalizeFolder(name)
	if IsSystemFolder(name) {
		return true, nil
	}
	return db.GetEngine(ctx).Where("user_id = ? AND name = ?", userID, name).Exist(new(Folder))
}

func EnsureSystemFolders(ctx context.Context, userID int64) error {
	for _, name := range SystemFolders {
		has, err := db.GetEngine(ctx).Where("user_id = ? AND name = ?", userID, name).Exist(new(Folder))
		if err != nil {
			return err
		}
		if has {
			continue
		}
		_, err = db.GetEngine(ctx).Insert(newFolder(userID, name))
		if err != nil {
			// A concurrent first access can create the same system folder.
			if exists, checkErr := db.GetEngine(ctx).Where("user_id = ? AND name = ?", userID, name).Exist(new(Folder)); checkErr == nil && exists {
				continue
			}
			return err
		}
	}
	return nil
}

func ListFolders(ctx context.Context, userID int64, subscribedOnly bool) ([]*Folder, error) {
	if err := EnsureSystemFolders(ctx, userID); err != nil {
		return nil, err
	}
	folders := make([]*Folder, 0, len(SystemFolders)+4)
	sess := db.GetEngine(ctx).Where("user_id = ?", userID).Asc("id")
	if subscribedOnly {
		sess = sess.And("subscribed = ?", true)
	}
	return folders, sess.Find(&folders)
}

func GetFolder(ctx context.Context, userID int64, name string) (*Folder, error) {
	name = NormalizeFolder(name)
	if err := EnsureSystemFolders(ctx, userID); err != nil {
		return nil, err
	}
	folder := &Folder{}
	has, err := db.GetEngine(ctx).Where("user_id = ? AND name = ?", userID, name).Get(folder)
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, fmt.Errorf("mailbox folder does not exist: %s", name)
	}
	return folder, nil
}

func CreateFolder(ctx context.Context, userID int64, name string) error {
	name = NormalizeFolder(name)
	if name == "" || len(name) > 255 || strings.ContainsRune(name, '\x00') {
		return errors.New("invalid mailbox folder name")
	}
	has, err := FolderExists(ctx, userID, name)
	if err != nil {
		return err
	}
	if has {
		return fmt.Errorf("mailbox folder already exists: %s", name)
	}
	_, err = db.GetEngine(ctx).Insert(newFolder(userID, name))
	return err
}

func DeleteFolder(ctx context.Context, userID int64, name string) error {
	name = NormalizeFolder(name)
	if name == "" {
		return errors.New("invalid mailbox folder name")
	}
	if IsSystemFolder(name) {
		return errors.New("system mailbox folders cannot be deleted")
	}
	if exists, err := FolderExists(ctx, userID, name); err != nil {
		return err
	} else if !exists {
		return fmt.Errorf("mailbox folder does not exist: %s", name)
	}
	return db.WithTx(ctx, func(ctx context.Context) error {
		msgs := make([]*Message, 0)
		if err := db.GetEngine(ctx).Where("user_id = ? AND folder = ?", userID, name).Find(&msgs); err != nil {
			return err
		}
		for _, msg := range msgs {
			if _, err := db.GetEngine(ctx).Where("user_id = ? AND message_id = ?", userID, msg.ID).Delete(new(Attachment)); err != nil {
				return err
			}
		}
		if _, err := db.GetEngine(ctx).Where("user_id = ? AND folder = ?", userID, name).Delete(new(Message)); err != nil {
			return err
		}
		_, err := db.GetEngine(ctx).Where("user_id = ? AND name = ?", userID, name).Delete(new(Folder))
		return err
	})
}

func RenameFolder(ctx context.Context, userID int64, oldName, newName string) error {
	oldName, newName = NormalizeFolder(oldName), NormalizeFolder(newName)
	if oldName == "" || newName == "" || len(newName) > 255 || strings.ContainsRune(newName, '\x00') {
		return errors.New("invalid mailbox folder name")
	}
	if IsSystemFolder(oldName) || IsSystemFolder(newName) {
		return errors.New("system mailbox folders cannot be renamed")
	}
	if strings.HasPrefix(newName, oldName+"/") {
		return errors.New("mailbox folder cannot be renamed below itself")
	}

	folders, err := ListFolders(ctx, userID, false)
	if err != nil {
		return err
	}
	moving := make(map[string]string)
	for _, folder := range folders {
		if folder.Name == oldName || strings.HasPrefix(folder.Name, oldName+"/") {
			suffix := strings.TrimPrefix(folder.Name, oldName)
			target := newName + suffix
			if len(target) > 255 {
				return fmt.Errorf("renamed mailbox folder is too long: %s", target)
			}
			moving[folder.Name] = target
		}
	}
	if _, ok := moving[oldName]; !ok {
		return fmt.Errorf("mailbox folder does not exist: %s", oldName)
	}
	for _, folder := range folders {
		if _, isMoving := moving[folder.Name]; isMoving {
			continue
		}
		for _, target := range moving {
			if folder.Name == target {
				return fmt.Errorf("mailbox folder already exists: %s", target)
			}
		}
	}

	return db.WithTx(ctx, func(ctx context.Context) error {
		// Rename deepest folders first. This avoids transient unique-index conflicts
		// when a hierarchy is moved as required by IMAP RENAME semantics.
		for depth := 255; depth >= 0; depth-- {
			for source, target := range moving {
				if len(source) != depth {
					continue
				}
				res, err := db.GetEngine(ctx).Where("user_id = ? AND name = ?", userID, source).Cols("name").Update(&Folder{Name: target})
				if err != nil {
					return err
				}
				if res == 0 {
					return fmt.Errorf("mailbox folder does not exist: %s", source)
				}
				if _, err = db.GetEngine(ctx).Where("user_id = ? AND folder = ?", userID, source).Cols("folder").Update(&Message{Folder: target}); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func SetFolderSubscribed(ctx context.Context, userID int64, name string, subscribed bool) error {
	name = NormalizeFolder(name)
	if err := EnsureSystemFolders(ctx, userID); err != nil {
		return err
	}
	_, err := db.GetEngine(ctx).Where("user_id = ? AND name = ?", userID, name).Cols("subscribed").Update(&Folder{Subscribed: subscribed})
	return err
}

func AllocateUID(ctx context.Context, userID int64, folder string) (uint32, error) {
	folder = NormalizeFolder(folder)
	if _, err := GetFolder(ctx, userID, folder); err != nil {
		return 0, err
	}
	for range 16 {
		f := &Folder{}
		has, err := db.GetEngine(ctx).Where("user_id = ? AND name = ?", userID, folder).Get(f)
		if err != nil {
			return 0, err
		}
		if !has {
			return 0, fmt.Errorf("mailbox folder does not exist: %s", folder)
		}
		current := f.UIDNext
		if current == 0 {
			current = 1
		}
		if current == ^uint32(0) {
			return 0, errors.New("mailbox UID space exhausted")
		}
		next := current + 1
		updated, err := db.GetEngine(ctx).Where("user_id = ? AND name = ? AND uid_next = ?", userID, folder, f.UIDNext).
			Cols("uid_next").Update(&Folder{UIDNext: next})
		if err != nil {
			return 0, err
		}
		if updated == 1 {
			return current, nil
		}
	}
	return 0, errors.New("could not allocate mailbox UID after concurrent updates")
}

func InsertMessage(ctx context.Context, msg *Message, attachments []*Attachment) error {
	if msg.UserID == 0 {
		return errors.New("mailbox message user is required")
	}
	msg.Folder = NormalizeFolder(msg.Folder)
	if msg.Folder == "" {
		msg.Folder = FolderInbox
	}
	if msg.InternetMessageID != "" && msg.InternetMessageHash == "" {
		msg.InternetMessageHash = hashInternetMessageID(msg.InternetMessageID)
	}
	return db.WithTx(ctx, func(ctx context.Context) error {
		if msg.UID == 0 {
			uid, err := AllocateUID(ctx, msg.UserID, msg.Folder)
			if err != nil {
				return err
			}
			msg.UID = uid
		}
		if msg.ReceivedUnix == 0 {
			msg.ReceivedUnix = timeutil.TimeStampNow()
		}
		if _, err := db.GetEngine(ctx).Insert(msg); err != nil {
			return err
		}
		for _, attachment := range attachments {
			attachment.MessageID = msg.ID
			attachment.UserID = msg.UserID
			if _, err := db.GetEngine(ctx).Insert(attachment); err != nil {
				return err
			}
		}
		return nil
	})
}

func GetMessage(ctx context.Context, userID, id int64) (*Message, error) {
	msg := &Message{}
	has, err := db.GetEngine(ctx).Where("user_id = ? AND id = ?", userID, id).Get(msg)
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, ErrMessageNotExist
	}
	return msg, nil
}

func GetMessageByInternetID(ctx context.Context, userID int64, folder, internetMessageID string) (*Message, error) {
	if internetMessageID == "" {
		return nil, ErrMessageNotExist
	}
	msg := &Message{}
	has, err := db.GetEngine(ctx).Where("user_id = ? AND folder = ? AND internet_message_hash = ? AND internet_message_id = ?",
		userID, NormalizeFolder(folder), hashInternetMessageID(internetMessageID), internetMessageID).Get(msg)
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, ErrMessageNotExist
	}
	return msg, nil
}

func hashInternetMessageID(messageID string) string {
	sum := sha256.Sum256([]byte(messageID))
	return hex.EncodeToString(sum[:])
}

func GetMessageByUID(ctx context.Context, userID int64, folder string, uid uint32) (*Message, error) {
	msg := &Message{}
	has, err := db.GetEngine(ctx).Where("user_id = ? AND folder = ? AND uid = ?", userID, NormalizeFolder(folder), uid).Get(msg)
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, ErrMessageNotExist
	}
	return msg, nil
}

func ListMessages(ctx context.Context, userID int64, folder, query string, limit, offset int) ([]*Message, int64, error) {
	folder = NormalizeFolder(folder)
	if folder == "" {
		folder = FolderInbox
	}
	var cond builder.Cond = builder.Eq{"user_id": userID, "folder": folder}
	if folder != FolderTrash {
		cond = cond.And(builder.Eq{"deleted": false})
	}
	if query != "" {
		q := "%" + strings.ToLower(strings.TrimSpace(query)) + "%"
		cond = cond.And(builder.Or(
			builder.Like{"LOWER(subject)", q},
			builder.Like{"LOWER(from_address)", q},
			builder.Like{"LOWER(from_name)", q},
			builder.Like{"LOWER(text_body)", q},
		))
	}
	count, err := db.GetEngine(ctx).Where(cond).Count(new(Message))
	if err != nil {
		return nil, 0, err
	}
	msgs := make([]*Message, 0, limit)
	err = db.GetEngine(ctx).Where(cond).Omit("raw", "html_body", "text_body").Desc("received_unix").Limit(limit, offset).Find(&msgs)
	return msgs, count, err
}

// ListFolderMessages returns every message in a folder in UID order, without the
// raw blob. IMAP clients list a whole folder on every poll, so the wire bytes are
// only paid for when a command actually needs them; load those with GetMessageRaw.
func ListFolderMessages(ctx context.Context, userID int64, folder string) ([]*Message, error) {
	msgs := make([]*Message, 0, 64)
	return msgs, db.GetEngine(ctx).Where("user_id = ? AND folder = ?", userID, NormalizeFolder(folder)).Omit("raw").Asc("uid").Find(&msgs)
}

// GetMessageRaw loads only the RFC 5322 bytes of one message.
func GetMessageRaw(ctx context.Context, userID, id int64) ([]byte, error) {
	msg := &Message{}
	has, err := db.GetEngine(ctx).Cols("raw").Where("user_id = ? AND id = ?", userID, id).Get(msg)
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, ErrMessageNotExist
	}
	return msg.Raw, nil
}

func GetAttachments(ctx context.Context, userID, messageID int64) ([]*Attachment, error) {
	attachments := make([]*Attachment, 0, 4)
	return attachments, db.GetEngine(ctx).Where("user_id = ? AND message_id = ?", userID, messageID).Asc("id").Find(&attachments)
}

func GetAttachment(ctx context.Context, userID, id int64) (*Attachment, error) {
	attachment := &Attachment{}
	has, err := db.GetEngine(ctx).Where("user_id = ? AND id = ?", userID, id).Get(attachment)
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, errors.New("mailbox attachment does not exist")
	}
	return attachment, nil
}

func SetFlags(ctx context.Context, userID, messageID int64, seen, flagged, answered, deleted *bool) error {
	msg, err := GetMessage(ctx, userID, messageID)
	if err != nil {
		return err
	}
	cols := make([]string, 0, 4)
	if seen != nil {
		msg.Seen = *seen
		msg.Recent = false
		cols = append(cols, "seen", "recent")
	}
	if flagged != nil {
		msg.Flagged = *flagged
		cols = append(cols, "flagged")
	}
	if answered != nil {
		msg.Answered = *answered
		cols = append(cols, "answered")
	}
	if deleted != nil {
		msg.Deleted = *deleted
		cols = append(cols, "deleted")
	}
	if len(cols) == 0 {
		return nil
	}
	_, err = db.GetEngine(ctx).Where("user_id = ? AND id = ?", userID, messageID).Cols(cols...).Update(msg)
	return err
}

func SetMessageIMAPFlags(ctx context.Context, userID, messageID int64, seen, flagged, answered, draft, deleted bool) error {
	_, err := db.GetEngine(ctx).Where("user_id = ? AND id = ?", userID, messageID).
		Cols("seen", "flagged", "answered", "draft", "deleted").
		Update(&Message{Seen: seen, Flagged: flagged, Answered: answered, Draft: draft, Deleted: deleted})
	return err
}

func SetMessageRecent(ctx context.Context, userID, messageID int64, recent bool) error {
	_, err := db.GetEngine(ctx).Where("user_id = ? AND id = ?", userID, messageID).Cols("recent").Update(&Message{Recent: recent})
	return err
}

func SetMessageInternalDate(ctx context.Context, userID, messageID int64, date timeutil.TimeStamp) error {
	_, err := db.GetEngine(ctx).Where("user_id = ? AND id = ?", userID, messageID).Cols("received_unix").Update(&Message{ReceivedUnix: date})
	return err
}

func MarkRead(ctx context.Context, userID, messageID int64, seen bool) error {
	_, err := db.GetEngine(ctx).Where("user_id = ? AND id = ?", userID, messageID).Cols("seen", "recent").Update(&Message{Seen: seen, Recent: false})
	return err
}

func SetFlagged(ctx context.Context, userID, messageID int64, flagged bool) error {
	_, err := db.GetEngine(ctx).Where("user_id = ? AND id = ?", userID, messageID).Cols("flagged").Update(&Message{Flagged: flagged})
	return err
}

func MoveMessage(ctx context.Context, userID, messageID int64, folder string) error {
	folder = NormalizeFolder(folder)
	if exists, err := FolderExists(ctx, userID, folder); err != nil {
		return err
	} else if !exists {
		return fmt.Errorf("mailbox folder does not exist: %s", folder)
	}
	uid, err := AllocateUID(ctx, userID, folder)
	if err != nil {
		return err
	}
	_, err = db.GetEngine(ctx).Where("user_id = ? AND id = ?", userID, messageID).Cols("folder", "uid", "deleted", "recent").Update(&Message{Folder: folder, UID: uid, Deleted: false, Recent: true})
	return err
}

func CopyMessage(ctx context.Context, userID, messageID int64, folder string) (*Message, error) {
	folder = NormalizeFolder(folder)
	if exists, err := FolderExists(ctx, userID, folder); err != nil {
		return nil, err
	} else if !exists {
		return nil, fmt.Errorf("mailbox folder does not exist: %s", folder)
	}
	src, err := GetMessage(ctx, userID, messageID)
	if err != nil {
		return nil, err
	}
	attachments, err := GetAttachments(ctx, userID, messageID)
	if err != nil {
		return nil, err
	}
	copyMsg := *src
	copyMsg.ID = 0
	copyMsg.Folder = folder
	copyMsg.UID = 0
	copyMsg.Recent = true
	copyAttachments := make([]*Attachment, 0, len(attachments))
	for _, a := range attachments {
		cp := *a
		cp.ID, cp.MessageID = 0, 0
		copyAttachments = append(copyAttachments, &cp)
	}
	if err := InsertMessage(ctx, &copyMsg, copyAttachments); err != nil {
		return nil, err
	}
	return &copyMsg, nil
}

func DeleteMessage(ctx context.Context, userID, messageID int64) error {
	msg, err := GetMessage(ctx, userID, messageID)
	if err != nil {
		return err
	}
	if msg.Folder != FolderTrash {
		return MoveMessage(ctx, userID, messageID, FolderTrash)
	}
	return PurgeMessage(ctx, userID, messageID)
}

func PurgeMessage(ctx context.Context, userID, messageID int64) error {
	return db.WithTx(ctx, func(ctx context.Context) error {
		if _, err := db.GetEngine(ctx).Where("user_id = ? AND message_id = ?", userID, messageID).Delete(new(Attachment)); err != nil {
			return err
		}
		_, err := db.GetEngine(ctx).Where("user_id = ? AND id = ?", userID, messageID).Delete(new(Message))
		return err
	})
}

func Expunge(ctx context.Context, userID int64, folder string) error {
	msgs := make([]*Message, 0)
	if err := db.GetEngine(ctx).Where("user_id = ? AND folder = ? AND deleted = ?", userID, NormalizeFolder(folder), true).Find(&msgs); err != nil {
		return err
	}
	for _, msg := range msgs {
		if err := PurgeMessage(ctx, userID, msg.ID); err != nil {
			return err
		}
	}
	return nil
}

func UsedBytes(ctx context.Context, userID int64) (int64, error) {
	return db.GetEngine(ctx).Where("user_id = ?", userID).SumInt(new(Message), "size")
}

func UnreadCount(ctx context.Context, userID int64) (int64, error) {
	return db.GetEngine(ctx).Where("user_id = ? AND folder = ? AND seen = ? AND deleted = ?", userID, FolderInbox, false, false).Count(new(Message))
}

func AddAlias(ctx context.Context, userID int64, localPart string) error {
	return addAliasOfKind(ctx, userID, localPart, AliasKindManual)
}

func addAliasOfKind(ctx context.Context, userID int64, localPart, kind string) error {
	localPart = strings.ToLower(strings.TrimSpace(localPart))
	if !validLocalPart(localPart) {
		return errors.New("invalid mailbox alias")
	}
	_, err := db.GetEngine(ctx).Insert(&Alias{UserID: userID, LocalPart: localPart, Kind: kind})
	return err
}

// RetireLocalPart binds a local-part an account is giving up to that same
// account, so the address keeps reaching its original owner and cannot be
// inherited by whoever claims the freed username next. It is a no-op when the
// local-part is already claimed, which covers a rename back and forth.
func RetireLocalPart(ctx context.Context, userID int64, localPart string) error {
	localPart = strings.ToLower(strings.TrimSpace(localPart))
	if !validLocalPart(localPart) {
		return nil
	}
	has, err := db.GetEngine(ctx).Where("local_part = ?", localPart).Exist(new(Alias))
	if err != nil || has {
		return err
	}
	return addAliasOfKind(ctx, userID, localPart, AliasKindRetired)
}

// ListAllAliases returns every address binding, for the administration page.
func ListAllAliases(ctx context.Context) ([]*Alias, error) {
	aliases := make([]*Alias, 0, 32)
	return aliases, db.GetEngine(ctx).Asc("local_part").Find(&aliases)
}

// DeleteAliasByID removes a binding regardless of owner, for administrators.
func DeleteAliasByID(ctx context.Context, id int64) error {
	_, err := db.GetEngine(ctx).Where("id = ?", id).Delete(new(Alias))
	return err
}

// LocalPartOwner reports which account owns a local-part, if any.
func LocalPartOwner(ctx context.Context, localPart string) (int64, bool, error) {
	alias, err := FindAlias(ctx, localPart)
	if err != nil {
		return 0, false, nil
	}
	return alias.UserID, true, nil
}

func DeleteAlias(ctx context.Context, userID, id int64) error {
	_, err := db.GetEngine(ctx).Where("user_id = ? AND id = ?", userID, id).Delete(new(Alias))
	return err
}

func ListAliases(ctx context.Context, userID int64) ([]*Alias, error) {
	aliases := make([]*Alias, 0, 4)
	return aliases, db.GetEngine(ctx).Where("user_id = ?", userID).Asc("local_part").Find(&aliases)
}

func FindAlias(ctx context.Context, localPart string) (*Alias, error) {
	alias := &Alias{}
	has, err := db.GetEngine(ctx).Where("local_part = ?", strings.ToLower(strings.TrimSpace(localPart))).Get(alias)
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, errors.New("mailbox alias does not exist")
	}
	return alias, nil
}

func validLocalPart(s string) bool {
	if s == "" || len(s) > 64 || strings.HasPrefix(s, ".") || strings.HasSuffix(s, ".") || strings.Contains(s, "..") {
		return false
	}
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || strings.ContainsRune(".!#$%&'*+-/=?^_`{|}~", r) {
			continue
		}
		return false
	}
	return true
}
