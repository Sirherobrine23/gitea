// Copyright 2023 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package incoming

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	net_mail "net/mail"
	"strings"
	"time"

	"gitea.dev/modules/log"
	"gitea.dev/modules/process"
	"gitea.dev/modules/setting"
	"gitea.dev/services/mailer/token"

	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapclient"
	"github.com/jhillyerd/enmime/v2"
)

func Init(ctx context.Context) error {
	if !setting.IncomingEmail.Enabled || setting.IncomingEmail.LocalDelivery {
		return nil
	}
	go func() {
		ctx, _, finished := process.GetManager().AddTypedContext(ctx, "Incoming Email", process.SystemProcessType, true)
		defer finished()

		// This background job processes incoming emails. It uses the IMAP IDLE command to get notified about incoming emails.
		// The following loop restarts the processing logic after errors until ctx indicates to stop.

		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := processIncomingEmails(ctx); err != nil {
					log.Error("Error while processing incoming emails: %v", err)
				}
				select {
				case <-ctx.Done():
					return
				case <-time.NewTimer(10 * time.Second).C:
				}
			}
		}
	}()

	return nil
}

// processIncomingEmails is the "main" method with the wait/process loop
func processIncomingEmails(ctx context.Context) error {
	server := fmt.Sprintf("%s:%d", setting.IncomingEmail.Host, setting.IncomingEmail.Port)

	// The server announces new mail as unilateral data while the client idles.
	// The handler is registered for the connection's lifetime, so it signals a
	// channel that waitForUpdates waits on.
	mailboxUpdates := make(chan struct{}, 1)
	options := &imapclient.Options{
		UnilateralDataHandler: &imapclient.UnilateralDataHandler{
			Mailbox: func(*imapclient.UnilateralDataMailbox) {
				select {
				case mailboxUpdates <- struct{}{}:
				default:
				}
			},
		},
	}

	var (
		c   *imapclient.Client
		err error
	)
	if setting.IncomingEmail.UseTLS {
		options.TLSConfig = &tls.Config{InsecureSkipVerify: setting.IncomingEmail.SkipTLSVerify} //nolint:gosec // configured by SKIP_TLS_VERIFY
		c, err = imapclient.DialTLS(server, options)
	} else {
		c, err = imapclient.DialInsecure(server, options)
	}
	if err != nil {
		return fmt.Errorf("could not connect to server '%s': %w", server, err)
	}
	defer c.Close()

	if err := c.Login(setting.IncomingEmail.Username, setting.IncomingEmail.Password).Wait(); err != nil {
		return fmt.Errorf("could not login: %w", err)
	}
	defer func() {
		if err := c.Logout().Wait(); err != nil {
			log.Error("Logout from incoming email server failed: %v", err)
		}
	}()

	if _, err := c.Select(setting.IncomingEmail.Mailbox, nil).Wait(); err != nil {
		return fmt.Errorf("selecting box '%s' failed: %w", setting.IncomingEmail.Mailbox, err)
	}

	// The following loop processes messages. If there are no messages available, IMAP IDLE is used to wait for new messages.
	// This process is repeated until an IMAP error occurs or ctx indicates to stop.

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if err := processMessages(ctx, c); err != nil {
				return fmt.Errorf("could not process messages: %w", err)
			}
			if err := waitForUpdates(ctx, c, mailboxUpdates); err != nil {
				return fmt.Errorf("wait for updates failed: %w", err)
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.NewTimer(time.Second).C:
			}
		}
	}
}

// waitForUpdates uses IMAP IDLE to wait for new emails
func waitForUpdates(ctx context.Context, c *imapclient.Client, mailboxUpdates <-chan struct{}) error {
	idle, err := c.Idle()
	if err != nil {
		return fmt.Errorf("imap idle failed: %w", err)
	}

	// Any mailbox update means there may be mail to process: stop idling and let
	// the caller run another search.
	select {
	case <-mailboxUpdates:
	case <-ctx.Done():
	}
	if err := idle.Close(); err != nil {
		return fmt.Errorf("imap idle close failed: %w", err)
	}
	return nil
}

// processMessages searches unread mails and processes them.
func processMessages(ctx context.Context, c *imapclient.Client) error {
	criteria := &imap.SearchCriteria{
		NotFlag: []imap.Flag{imap.FlagSeen},
	}
	if setting.IncomingEmail.MaximumMessageSize > 0 {
		criteria.Smaller = int64(setting.IncomingEmail.MaximumMessageSize)
	}
	searchData, err := c.UIDSearch(criteria, nil).Wait()
	if err != nil {
		return fmt.Errorf("imap search failed: %w", err)
	}
	uids, ok := searchData.All.(imap.UIDSet)
	if !ok || len(uids) == 0 {
		return nil
	}

	section := &imap.FetchItemBodySection{Peek: true}
	fetchCmd := c.Fetch(uids, &imap.FetchOptions{
		UID:         true,
		BodySection: []*imap.FetchItemBodySection{section},
	})
	defer fetchCmd.Close()

	var handled imap.UIDSet
	for {
		if ctx.Err() != nil {
			return nil
		}
		msg := fetchCmd.Next()
		if msg == nil {
			break
		}
		buffer, err := msg.Collect()
		if err != nil {
			return fmt.Errorf("imap fetch failed: %w", err)
		}

		body := buffer.FindBodySection(section)
		if body == nil {
			log.Error("Error while processing incoming email[%v]: could not get body from message", buffer.UID)
			continue
		}
		wasHandled, err := HandleReader(ctx, bytes.NewReader(body))
		if err != nil {
			log.Error("Error while processing incoming email[%v]: %v", buffer.UID, err)
			continue
		}
		if wasHandled {
			handled.AddNum(buffer.UID)
		}
	}
	if err := fetchCmd.Close(); err != nil {
		return fmt.Errorf("imap fetch failed: %w", err)
	}

	if setting.IncomingEmail.DeleteHandledMessage && len(handled) > 0 {
		storeFlags := &imap.StoreFlags{Op: imap.StoreFlagsAdd, Silent: true, Flags: []imap.Flag{imap.FlagDeleted}}
		if err := c.Store(handled, storeFlags, nil).Close(); err != nil {
			return fmt.Errorf("imap store failed: %w", err)
		}
		if err := c.Expunge().Close(); err != nil {
			return fmt.Errorf("imap expunge failed: %w", err)
		}
	}

	return nil
}

// HandleReader parses and dispatches a Gitea tokenized incoming message. It returns
// true only when a registered incoming-mail handler consumed the message. SMTP local
// delivery uses this entry point so reply-by-email does not need an external IMAP hop.
func HandleReader(ctx context.Context, r io.Reader) (bool, error) {
	env, err := enmime.ReadEnvelope(r)
	if err != nil {
		return false, fmt.Errorf("could not read envelope: %w", err)
	}
	return HandleEnvelope(ctx, env)
}

// HandleEnvelope dispatches an already parsed message to the reply/unsubscribe handler.
func HandleEnvelope(ctx context.Context, env *enmime.Envelope) (bool, error) {
	return handleEnvelopeToken(ctx, env, searchTokenInHeaders(env))
}

// HandleReaderForAddress dispatches a message using the SMTP envelope recipient.
// This makes local delivery work even when the tokenized recipient is not present
// in the RFC 5322 To header (for example after forwarding or Bcc delivery).
func HandleReaderForAddress(ctx context.Context, r io.Reader, address string) (bool, error) {
	env, err := enmime.ReadEnvelope(r)
	if err != nil {
		return false, fmt.Errorf("could not read envelope: %w", err)
	}
	return handleEnvelopeToken(ctx, env, handlerTokenFromAddress(address))
}

func handleEnvelopeToken(ctx context.Context, env *enmime.Envelope, t string) (bool, error) {
	if isAutomaticReply(env) {
		log.Debug("Skipping automatic email reply")
		return false, nil
	}
	if t == "" {
		return false, nil
	}

	handlerType, user, payload, err := token.DecodeToken(ctx, t)
	if err != nil {
		if _, ok := err.(*token.ErrToken); ok {
			log.Info("Invalid incoming email token: %v", err)
			return false, nil
		}
		return false, err
	}

	handler, ok := handlers[handlerType]
	if !ok {
		return false, fmt.Errorf("unexpected handler type: %v", handlerType)
	}
	if err := handler.Handle(ctx, getContentFromMailReader(env), user, payload); err != nil {
		return false, fmt.Errorf("could not handle message: %w", err)
	}
	return true, nil
}

func handlerTokenFromAddress(address string) string {
	if !setting.IncomingEmail.Enabled || setting.IncomingEmail.ReplyToAddress == "" {
		return ""
	}
	prefix, suffix, ok := strings.Cut(setting.IncomingEmail.ReplyToAddress, setting.IncomingEmailTokenPlaceholder)
	if !ok {
		return ""
	}
	return extractToken(address, prefix, suffix)
}

// IsHandlerAddress reports whether address matches the configured tokenized reply-to pattern.
func IsHandlerAddress(address string) bool {
	return handlerTokenFromAddress(address) != ""
}

// ValidateHandlerAddress verifies that a tokenized SMTP recipient decodes to a
// currently registered Gitea incoming-email handler.
func ValidateHandlerAddress(ctx context.Context, address string) error {
	t := handlerTokenFromAddress(address)
	if t == "" {
		return errors.New("address is not a Gitea incoming-email handler address")
	}
	handlerType, _, _, err := token.DecodeToken(ctx, t)
	if err != nil {
		return err
	}
	if _, ok := handlers[handlerType]; !ok {
		return fmt.Errorf("unexpected handler type: %v", handlerType)
	}
	return nil
}

// isAutomaticReply tests if the headers indicate an automatic reply
func isAutomaticReply(env *enmime.Envelope) bool {
	autoSubmitted := env.GetHeader("Auto-Submitted")
	if autoSubmitted != "" && autoSubmitted != "no" {
		return true
	}
	autoReply := env.GetHeader("X-Autoreply")
	if autoReply == "yes" {
		return true
	}
	autoRespond := env.GetHeader("X-Autorespond")
	return autoRespond != ""
}

func extractToken(s, tokenPrefix, tokenSuffix string) string {
	if len(s) <= len(tokenPrefix)+len(tokenSuffix) {
		return ""
	}
	prefix, suffix := s[0:len(tokenPrefix)], s[len(s)-len(tokenSuffix):]
	if strings.EqualFold(prefix, tokenPrefix) && strings.EqualFold(suffix, tokenSuffix) {
		return s[len(tokenPrefix) : len(s)-len(tokenSuffix)]
	}
	return ""
}

// searchTokenInHeaders looks for the token in To, Delivered-To and References
func searchTokenInHeaders(env *enmime.Envelope) string {
	to, _ := env.AddressList("To")

	token := searchTokenInAddresses(to)
	if token != "" {
		return token
	}

	deliveredTo, _ := env.AddressList("Delivered-To")

	token = searchTokenInAddresses(deliveredTo)
	if token != "" {
		return token
	}

	references := env.GetHeader("References")
	for {
		begin := strings.IndexByte(references, '<')
		if begin == -1 {
			break
		}
		begin++

		end := strings.IndexByte(references, '>')
		if end == -1 || begin > end {
			break
		}
		t := extractToken(references[begin:end], "reply-", "@"+setting.Domain)
		if t != "" {
			return t
		}

		references = references[end+1:]
	}

	return ""
}

// searchTokenInAddresses looks for the token in an address
func searchTokenInAddresses(addresses []*net_mail.Address) string {
	tokenPrefix, tokenSuffix, _ := strings.Cut(setting.IncomingEmail.ReplyToAddress, setting.IncomingEmailTokenPlaceholder)
	if tokenSuffix == "" {
		return ""
	}
	for _, address := range addresses {
		if t := extractToken(address.Address, tokenPrefix, tokenSuffix); t != "" {
			return t
		}
	}
	return ""
}

type MailContent struct {
	Content     string
	Attachments []*Attachment
}

type Attachment struct {
	Name    string
	Content []byte
}

// getContentFromMailReader grabs the plain content and the attachments from the mail.
// A potential reply/signature gets stripped from the content.
func getContentFromMailReader(env *enmime.Envelope) *MailContent {
	attachments := make([]*Attachment, 0, len(env.Attachments))
	for _, attachment := range env.Attachments {
		attachments = append(attachments, &Attachment{
			Name:    attachment.FileName,
			Content: attachment.Content,
		})
	}

	return &MailContent{
		Content:     extractReply(env.Text),
		Attachments: attachments,
	}
}
