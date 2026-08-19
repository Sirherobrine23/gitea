// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package mailbox

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"html"
	"io"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net/mail"
	"net/textproto"
	"path/filepath"
	"strings"
	"time"

	mailbox_model "gitea.dev/models/mailbox"
	user_model "gitea.dev/models/user"
	"gitea.dev/modules/log"
	"gitea.dev/modules/setting"
	"gitea.dev/modules/timeutil"
	incoming_service "gitea.dev/services/mailer/incoming"
	sender_service "gitea.dev/services/mailer/sender"

	"github.com/jhillyerd/enmime/v2"
	xhtml "golang.org/x/net/html"
)

var (
	ErrNotLocalRecipient = errors.New("recipient is not hosted by this Gitea instance")
	ErrRelayDisabled     = errors.New("mail relay is disabled")
	ErrQuotaExceeded     = errors.New("mailbox quota exceeded")
)

type DeliveryResult struct {
	LocalUsers  int
	RemoteUsers int
	Handled     bool
}

type ComposeAttachment struct {
	Filename    string
	ContentType string
	Content     []byte
}

type rawMessage []byte

func (m rawMessage) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(m)
	return int64(n), err
}

func Domain() string {
	return strings.ToLower(strings.TrimSpace(setting.MailboxServer.Domain))
}

// AddressForUser returns the account's own address, or "" when it has none.
// A username only yields an address when no alias claims that local-part: after
// a rename the old local-part stays bound to its original owner, so an account
// that later claims the freed username does not inherit their mail identity.
func AddressForUser(ctx context.Context, user *user_model.User) string {
	if user == nil {
		return ""
	}
	local := strings.ToLower(user.Name)
	if owner, claimed, err := mailbox_model.LocalPartOwner(ctx, local); err == nil && claimed && owner != user.ID {
		return ""
	}
	return local + "@" + Domain()
}

func IsLocalAddress(address string) bool {
	parsed, err := mail.ParseAddress(strings.TrimSpace(address))
	if err != nil {
		return false
	}
	_, domain, ok := strings.Cut(strings.ToLower(parsed.Address), "@")
	return ok && domain == Domain()
}

func ResolveRecipient(ctx context.Context, address string) (*user_model.User, error) {
	parsed, err := mail.ParseAddress(strings.TrimSpace(address))
	if err != nil {
		return nil, err
	}
	local, domain, ok := strings.Cut(strings.ToLower(parsed.Address), "@")
	if !ok || domain != Domain() {
		return nil, ErrNotLocalRecipient
	}

	// Plus addressing is delivered to the base account/alias. Tokenized Gitea
	// reply addresses are handled separately by incoming_service.
	baseLocal := local
	if i := strings.IndexByte(baseLocal, '+'); i >= 0 {
		baseLocal = baseLocal[:i]
	}

	// The alias table is the authority for who owns an address. Only an
	// administrator can write to it, so it cannot be used to shadow an account,
	// and a local-part retired by a rename keeps reaching its original owner
	// instead of whoever claims the freed username next.
	if alias, err := mailbox_model.FindAlias(ctx, local); err == nil {
		return deliverableUser(ctx, alias.UserID)
	}
	if alias, err := mailbox_model.FindAlias(ctx, baseLocal); err == nil {
		return deliverableUser(ctx, alias.UserID)
	}

	if user, err := user_model.GetIndividualUserByName(ctx, baseLocal); err == nil {
		if !user.IsActive || user.ProhibitLogin {
			return nil, ErrNotLocalRecipient
		}
		return user, nil
	}

	// Also allow a verified Gitea address on the hosted domain to be used as an
	// inbound alias even when its local-part differs from the username.
	user, err := user_model.GetUserByEmail(ctx, parsed.Address)
	if err == nil && user != nil && user.IsIndividual() && user.IsActive && !user.ProhibitLogin {
		return user, nil
	}

	// RFC 2142 requires postmaster and abuse to be reachable on a public domain.
	if setting.MailboxServer.PostmasterUser != "" && isRoleAddress(baseLocal) {
		if user, err := roleUser(ctx, setting.MailboxServer.PostmasterUser); err == nil {
			return user, nil
		}
	}
	if setting.MailboxServer.CatchAllUser != "" {
		if user, err := roleUser(ctx, setting.MailboxServer.CatchAllUser); err == nil {
			return user, nil
		}
	}
	return nil, ErrNotLocalRecipient
}

func isRoleAddress(localPart string) bool {
	return localPart == "postmaster" || localPart == "abuse"
}

func roleUser(ctx context.Context, username string) (*user_model.User, error) {
	user, err := user_model.GetIndividualUserByName(ctx, username)
	if err != nil {
		// Warn rather than error: a dictionary attack would otherwise flood the log
		// on every unknown recipient when the configured account no longer exists.
		log.Warn("Mailbox role account %q cannot be resolved: %v", username, err)
		return nil, err
	}
	if !user.IsActive || user.ProhibitLogin {
		return nil, ErrNotLocalRecipient
	}
	return user, nil
}

func deliverableUser(ctx context.Context, userID int64) (*user_model.User, error) {
	user, err := user_model.GetUserByID(ctx, userID)
	if err != nil {
		return nil, err
	}
	if !user.IsIndividual() || !user.IsActive || user.ProhibitLogin {
		return nil, ErrNotLocalRecipient
	}
	return user, nil
}

func CanAcceptRecipient(ctx context.Context, address string, allowRelay bool) error {
	parsed, err := mail.ParseAddress(strings.TrimSpace(address))
	if err != nil {
		return err
	}
	if incoming_service.IsHandlerAddress(parsed.Address) {
		return incoming_service.ValidateHandlerAddress(ctx, parsed.Address)
	}
	if _, err := ResolveRecipient(ctx, parsed.Address); err == nil {
		return nil
	}
	if IsLocalAddress(parsed.Address) {
		return ErrNotLocalRecipient
	}
	if allowRelay && setting.MailboxServer.RelayEnabled {
		return nil
	}
	if allowRelay {
		return ErrRelayDisabled
	}
	return ErrNotLocalRecipient
}

// SignInFunc validates a username/password pair for SMTP and IMAP logins.
type SignInFunc func(ctx context.Context, username, password string) (*user_model.User, error)

var signInFunc SignInFunc

// SetSignInFunc wires the password authenticator used by SMTP AUTH and IMAP LOGIN.
// It is injected instead of imported because services/auth transitively pulls in
// services/mailer, which imports this package for local delivery.
func SetSignInFunc(fn SignInFunc) {
	signInFunc = fn
}

func Authenticate(ctx context.Context, username, password string) (*user_model.User, error) {
	if signInFunc == nil {
		return nil, errors.New("mailbox authenticator is not initialized")
	}
	user, err := signInFunc(ctx, username, password)
	if err != nil {
		return nil, err
	}
	if user == nil || !user.IsIndividual() || !user.IsActive || user.ProhibitLogin {
		return nil, errors.New("mailbox authentication rejected")
	}
	return user, nil
}

func SenderAllowed(ctx context.Context, user *user_model.User, from string) bool {
	if user == nil {
		return false
	}
	parsed, err := mail.ParseAddress(strings.TrimSpace(from))
	if err != nil {
		return false
	}
	if own := AddressForUser(ctx, user); own != "" && strings.EqualFold(parsed.Address, own) {
		return true
	}
	if IsLocalAddress(parsed.Address) {
		local, _, _ := strings.Cut(strings.ToLower(parsed.Address), "@")
		if alias, err := mailbox_model.FindAlias(ctx, local); err == nil && alias.UserID == user.ID {
			return true
		}
	}
	resolved, err := user_model.GetUserByEmail(ctx, parsed.Address)
	return err == nil && resolved != nil && resolved.ID == user.ID
}

func validateMessageFrom(ctx context.Context, user *user_model.User, raw []byte) error {
	message, err := mail.ReadMessage(bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("parse RFC5322 message: %w", err)
	}
	from, err := message.Header.AddressList("From")
	if err != nil {
		return fmt.Errorf("parse RFC5322.From: %w", err)
	}
	if len(from) != 1 {
		return fmt.Errorf("RFC5322.From must contain exactly one mailbox, got %d", len(from))
	}
	if !SenderAllowed(ctx, user, from[0].Address) {
		return errors.New("RFC5322.From address is not owned by the authenticated user")
	}
	return nil
}

// DeliveryOptions controls how a raw message is routed after SMTP authentication.
type DeliveryOptions struct {
	AllowRelay   bool
	LocalFolder  string
	SkipHandlers bool
	// SenderID owns any delivery status notice raised for this message. Zero for
	// mail this instance did not originate.
	SenderID int64
}

// DeliverRaw stores all local recipient copies, dispatches tokenized Gitea replies,
// and relays non-local recipients through the existing [mailer] transport when allowed.
func DeliverRaw(ctx context.Context, envelopeFrom string, recipients []string, raw []byte, allowRelay bool) (*DeliveryResult, error) {
	return DeliverRawWithOptions(ctx, envelopeFrom, recipients, raw, DeliveryOptions{AllowRelay: allowRelay})
}

// DeliverRawWithOptions is DeliverRaw with explicit local-folder and handler controls.
// SkipHandlers is used for DMARC-quarantined mail so an unauthenticated message cannot
// mutate an issue or pull request while it is being quarantined.
func DeliverRawWithOptions(ctx context.Context, envelopeFrom string, recipients []string, raw []byte, opts DeliveryOptions) (*DeliveryResult, error) {
	if setting.MailboxServer.MaxMessageSize > 0 && int64(len(raw)) > setting.MailboxServer.MaxMessageSize {
		return nil, fmt.Errorf("message exceeds configured limit of %d bytes", setting.MailboxServer.MaxMessageSize)
	}
	if len(recipients) == 0 {
		return nil, errors.New("no recipients")
	}
	if len(recipients) > setting.MailboxServer.MaxRecipients {
		return nil, fmt.Errorf("too many recipients: maximum is %d", setting.MailboxServer.MaxRecipients)
	}

	result := &DeliveryResult{}
	localUsers := map[int64]*user_model.User{}
	remote := make([]string, 0)
	handlerRecipients := make([]string, 0, 1)
	handlerSeen := make(map[string]struct{})

	for _, recipient := range recipients {
		parsed, err := mail.ParseAddress(recipient)
		if err != nil {
			return nil, fmt.Errorf("invalid recipient %q: %w", recipient, err)
		}
		if incoming_service.IsHandlerAddress(parsed.Address) {
			if err := incoming_service.ValidateHandlerAddress(ctx, parsed.Address); err != nil {
				return nil, fmt.Errorf("invalid Gitea incoming-email recipient %q: %w", parsed.Address, err)
			}
			key := strings.ToLower(parsed.Address)
			if _, ok := handlerSeen[key]; !ok {
				handlerSeen[key] = struct{}{}
				handlerRecipients = append(handlerRecipients, parsed.Address)
			}
			continue
		}
		user, err := ResolveRecipient(ctx, parsed.Address)
		if err == nil {
			localUsers[user.ID] = user
			continue
		}
		if !opts.AllowRelay {
			return nil, fmt.Errorf("%w: %s", ErrNotLocalRecipient, parsed.Address)
		}
		if !setting.MailboxServer.RelayEnabled {
			return nil, ErrRelayDisabled
		}
		remote = append(remote, parsed.Address)
	}

	// A single MIME parse is used as the source for all local mailbox copies.
	var env *enmime.Envelope
	if len(localUsers) > 0 {
		var err error
		env, err = enmime.ReadEnvelope(bytes.NewReader(raw))
		if err != nil {
			return nil, fmt.Errorf("parse MIME message: %w", err)
		}
	}

	localFolder := mailbox_model.NormalizeFolder(opts.LocalFolder)
	if localFolder == "" {
		localFolder = mailbox_model.FolderInbox
	}
	for _, user := range localUsers {
		if err := storeEnvelope(ctx, user, localFolder, raw, env, false); err != nil {
			return nil, err
		}
		result.LocalUsers++
	}

	if !opts.SkipHandlers {
		for _, recipient := range handlerRecipients {
			handled, err := incoming_service.HandleReaderForAddress(ctx, bytes.NewReader(raw), recipient)
			if err != nil {
				return nil, err
			}
			result.Handled = result.Handled || handled
		}
	}

	if len(remote) > 0 {
		if err := SendRemote(ctx, opts.SenderID, envelopeFrom, remote, raw); err != nil {
			return nil, err
		}
		result.RemoteUsers = len(remote)
	}

	return result, nil
}

// SendRemote hands a message to the configured outbound path. In direct mode it
// is queued for delivery to the recipient's own MX, so no [mailer] transport is
// needed; in relay mode it goes to the [mailer] transport as before.
func SendRemote(ctx context.Context, senderID int64, envelopeFrom string, recipients []string, raw []byte) error {
	if setting.MailboxServer.OutboundMode == setting.OutboundModeDirect {
		if err := QueueOutbound(ctx, senderID, envelopeFrom, recipients, raw); err != nil {
			return fmt.Errorf("queue external mail: %w", err)
		}
		return nil
	}
	if setting.MailService == nil {
		return errors.New("cannot relay external mail: OUTBOUND_MODE is relay but [mailer] is not enabled")
	}
	var relay sender_service.Sender
	switch setting.MailService.Protocol {
	case "sendmail":
		relay = &sender_service.SendmailSender{}
	case "dummy":
		relay = &sender_service.DummySender{}
	default:
		relay = &sender_service.SMTPSender{}
	}
	if err := relay.Send(envelopeFrom, recipients, rawMessage(raw)); err != nil {
		return fmt.Errorf("relay external mail: %w", err)
	}
	return nil
}

func StoreRaw(ctx context.Context, user *user_model.User, folder string, raw []byte, seen bool) (*mailbox_model.Message, error) {
	env, err := enmime.ReadEnvelope(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	return storeEnvelopeReturn(ctx, user, folder, raw, env, seen)
}

func storeEnvelope(ctx context.Context, user *user_model.User, folder string, raw []byte, env *enmime.Envelope, seen bool) error {
	_, err := storeEnvelopeReturn(ctx, user, folder, raw, env, seen)
	return err
}

func storeEnvelopeReturn(ctx context.Context, user *user_model.User, folder string, raw []byte, env *enmime.Envelope, seen bool) (*mailbox_model.Message, error) {
	if user == nil {
		return nil, errors.New("mailbox owner is nil")
	}

	internetMessageID := env.GetHeader("Message-ID")
	if internetMessageID != "" {
		existing, err := mailbox_model.GetMessageByInternetID(ctx, user.ID, folder, internetMessageID)
		if err == nil {
			return existing, nil
		}
		if !errors.Is(err, mailbox_model.ErrMessageNotExist) {
			return nil, err
		}
	}

	if setting.MailboxServer.DefaultQuota > 0 {
		used, err := mailbox_model.UsedBytes(ctx, user.ID)
		if err != nil {
			return nil, err
		}
		if used+int64(len(raw)) > setting.MailboxServer.DefaultQuota {
			return nil, ErrQuotaExceeded
		}
	}

	msg := &mailbox_model.Message{
		UserID:            user.ID,
		Folder:            mailbox_model.NormalizeFolder(folder),
		InternetMessageID: internetMessageID,
		InReplyTo:         env.GetHeader("In-Reply-To"),
		References:        env.GetHeader("References"),
		To:                env.GetHeader("To"),
		Cc:                env.GetHeader("Cc"),
		Bcc:               env.GetHeader("Bcc"),
		ReplyTo:           env.GetHeader("Reply-To"),
		Subject:           env.GetHeader("Subject"),
		TextBody:          env.Text,
		HTMLBody:          env.HTML,
		Raw:               append([]byte(nil), raw...),
		Size:              int64(len(raw)),
		Seen:              seen,
		Recent:            !seen,
		HasAttach:         len(env.Attachments) > 0,
		ReceivedUnix:      timeutil.TimeStampNow(),
	}
	if msg.TextBody == "" && msg.HTMLBody != "" {
		msg.TextBody = htmlToText(msg.HTMLBody)
	}
	if from, err := env.AddressList("From"); err == nil && len(from) > 0 {
		msg.FromName, msg.FromAddress = from[0].Name, from[0].Address
	}
	if date := env.GetHeader("Date"); date != "" {
		if t, err := mail.ParseDate(date); err == nil {
			msg.SentUnix = timeutil.TimeStamp(t.Unix())
		}
	}

	attachments := make([]*mailbox_model.Attachment, 0, len(env.Attachments))
	for _, part := range env.Attachments {
		contentType := part.ContentType
		if contentType == "" {
			contentType = "application/octet-stream"
		}
		attachments = append(attachments, &mailbox_model.Attachment{
			Filename:    part.FileName,
			ContentType: contentType,
			ContentID:   part.ContentID,
			Disposition: part.Disposition,
			Size:        int64(len(part.Content)),
			Content:     append([]byte(nil), part.Content...),
		})
	}
	if err := mailbox_model.InsertMessage(ctx, msg, attachments); err != nil {
		return nil, err
	}
	return msg, nil
}

func htmlToText(source string) string {
	doc, err := xhtml.Parse(strings.NewReader(source))
	if err != nil {
		return html.UnescapeString(source)
	}
	var b strings.Builder
	var walk func(*xhtml.Node)
	walk = func(n *xhtml.Node) {
		if n.Type == xhtml.TextNode {
			text := strings.TrimSpace(n.Data)
			if text != "" {
				if b.Len() > 0 {
					b.WriteByte(' ')
				}
				b.WriteString(text)
			}
		}
		if n.Type == xhtml.ElementNode && (n.Data == "br" || n.Data == "p" || n.Data == "div" || n.Data == "li") {
			b.WriteByte('\n')
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(doc)
	return strings.TrimSpace(b.String())
}

func AddAlias(ctx context.Context, user *user_model.User, localPart string) error {
	if user == nil {
		return errors.New("mailbox owner is nil")
	}
	localPart = strings.ToLower(strings.TrimSpace(localPart))
	address := localPart + "@" + Domain()
	if incoming_service.IsHandlerAddress(address) {
		return errors.New("alias conflicts with the Gitea incoming-email handler")
	}
	if existing, err := user_model.GetIndividualUserByName(ctx, localPart); err == nil && existing != nil && existing.ID != user.ID {
		return errors.New("alias conflicts with an existing Gitea username")
	}
	if owner, claimed, err := mailbox_model.LocalPartOwner(ctx, localPart); err == nil && claimed {
		if owner == user.ID {
			return errors.New("the account already owns this address")
		}
		return errors.New("address is already assigned to another account")
	}
	if existing, err := user_model.GetUserByEmail(ctx, address); err == nil && existing != nil {
		return errors.New("alias conflicts with an existing Gitea email address")
	}
	return mailbox_model.AddAlias(ctx, user.ID, localPart)
}

func ParseRecipientList(values ...string) ([]string, error) {
	out := make([]string, 0)
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			continue
		}
		addresses, err := mail.ParseAddressList(value)
		if err != nil {
			return nil, err
		}
		for _, address := range addresses {
			out = append(out, address.Address)
		}
	}
	return out, nil
}

func ComposeAndSend(ctx context.Context, user *user_model.User, to, cc, bcc []string, subject, body string, attachments []ComposeAttachment) (*DeliveryResult, error) {
	if user == nil {
		return nil, errors.New("sender is nil")
	}
	allRecipients := append(append(append([]string{}, to...), cc...), bcc...)
	if len(allRecipients) == 0 {
		return nil, errors.New("at least one recipient is required")
	}
	from := AddressForUser(ctx, user)
	raw, err := BuildMessage(user.DisplayName(), from, to, cc, bcc, subject, body, attachments)
	if err != nil {
		return nil, err
	}
	raw, err = SignOutboundDKIM(raw)
	if err != nil {
		return nil, fmt.Errorf("sign outgoing message with DKIM: %w", err)
	}
	result, err := DeliverRawWithOptions(ctx, from, allRecipients, raw, DeliveryOptions{AllowRelay: true, SenderID: user.ID})
	if err != nil {
		return nil, err
	}
	if _, err := StoreRaw(ctx, user, mailbox_model.FolderSent, raw, true); err != nil {
		log.Error("Failed to store sent-mail copy for user %d: %v", user.ID, err)
	}
	return result, nil
}

func BuildMessage(fromName, from string, to, cc, bcc []string, subject, body string, attachments []ComposeAttachment) ([]byte, error) {
	fromAddr := &mail.Address{Name: fromName, Address: from}
	if _, err := mail.ParseAddress(fromAddr.String()); err != nil {
		return nil, err
	}
	for _, recipient := range append(append(append([]string{}, to...), cc...), bcc...) {
		if _, err := mail.ParseAddress(recipient); err != nil {
			return nil, fmt.Errorf("invalid recipient %q: %w", recipient, err)
		}
	}

	var out bytes.Buffer
	writeHeader := func(name, value string) {
		value = strings.NewReplacer("\r", "", "\n", "").Replace(value)
		fmt.Fprintf(&out, "%s: %s\r\n", name, value)
	}
	writeHeader("Date", time.Now().Format(time.RFC1123Z))
	writeHeader("Message-ID", newMessageID())
	writeHeader("From", fromAddr.String())
	if len(to) > 0 {
		writeHeader("To", strings.Join(to, ", "))
	}
	if len(cc) > 0 {
		writeHeader("Cc", strings.Join(cc, ", "))
	}
	// Bcc is deliberately envelope-only and must never be written into the message.
	writeHeader("Subject", mime.QEncoding.Encode("utf-8", subject))
	writeHeader("MIME-Version", "1.0")
	writeHeader("X-Mailer", "Gitea Mailbox")

	if len(attachments) == 0 {
		writeHeader("Content-Type", `text/plain; charset="utf-8"`)
		writeHeader("Content-Transfer-Encoding", "quoted-printable")
		out.WriteString("\r\n")
		qp := quotedprintable.NewWriter(&out)
		_, _ = io.WriteString(qp, body)
		if err := qp.Close(); err != nil {
			return nil, err
		}
		return out.Bytes(), nil
	}

	mw := multipart.NewWriter(&out)
	writeHeader("Content-Type", fmt.Sprintf(`multipart/mixed; boundary="%s"`, mw.Boundary()))
	out.WriteString("\r\n")

	textHeader := textproto.MIMEHeader{}
	textHeader.Set("Content-Type", `text/plain; charset="utf-8"`)
	textHeader.Set("Content-Transfer-Encoding", "quoted-printable")
	part, err := mw.CreatePart(textHeader)
	if err != nil {
		return nil, err
	}
	qp := quotedprintable.NewWriter(part)
	_, _ = io.WriteString(qp, body)
	if err := qp.Close(); err != nil {
		return nil, err
	}

	for _, attachment := range attachments {
		filename := filepath.Base(strings.TrimSpace(attachment.Filename))
		if filename == "." || filename == "" {
			filename = "attachment"
		}
		contentType := attachment.ContentType
		if contentType == "" {
			contentType = "application/octet-stream"
		}
		h := textproto.MIMEHeader{}
		h.Set("Content-Type", mime.FormatMediaType(contentType, map[string]string{"name": filename}))
		h.Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": filename}))
		h.Set("Content-Transfer-Encoding", "base64")
		part, err := mw.CreatePart(h)
		if err != nil {
			return nil, err
		}
		encoded := base64.StdEncoding.EncodeToString(attachment.Content)
		for len(encoded) > 76 {
			_, _ = io.WriteString(part, encoded[:76]+"\r\n")
			encoded = encoded[76:]
		}
		if encoded != "" {
			_, _ = io.WriteString(part, encoded+"\r\n")
		}
	}
	if err := mw.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func newMessageID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("<%d@gitea.%s>", time.Now().UnixNano(), Domain())
	}
	return fmt.Sprintf("<%s@gitea.%s>", hex.EncodeToString(buf), Domain())
}
