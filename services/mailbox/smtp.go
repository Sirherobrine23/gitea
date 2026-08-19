// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package mailbox

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	mailbox_model "gitea.dev/models/mailbox"
	user_model "gitea.dev/models/user"
	"gitea.dev/modules/log"
	"gitea.dev/modules/setting"

	"github.com/emersion/go-sasl"
	"github.com/emersion/go-smtp"
)

const (
	smtpReadTimeout  = 10 * time.Minute
	smtpWriteTimeout = 5 * time.Minute
	// RFC 5321 caps text lines at 1000 octets. Real senders exceed it, so the
	// limit is raised while still bounding what a single line can buffer.
	smtpMaxLineLength = 1024 * 1024
)

type smtpListenerConfig struct {
	addr        string
	requireAuth bool
	implicitTLS bool
	name        string
}

// smtpBackend serves every listener. requireAuth marks the submission ports,
// where a session must authenticate before it may set a return path.
type smtpBackend struct {
	ctx         context.Context
	requireAuth bool
}

type smtpSession struct {
	ctx         context.Context
	conn        *smtp.Conn
	requireAuth bool
	user        *user_model.User
	mailFrom    string
	recipients  []string
}

func initSMTP(ctx context.Context, tlsConfig *tls.Config) error {
	configs := []smtpListenerConfig{
		{addr: setting.MailboxServer.SMTPListen, requireAuth: false, name: "SMTP"},
		{addr: setting.MailboxServer.SMTPSubmissionListen, requireAuth: true, name: "SMTP submission"},
		{addr: setting.MailboxServer.SMTPSListen, requireAuth: true, implicitTLS: true, name: "SMTPS"},
	}
	seen := map[string]bool{}
	for _, cfg := range configs {
		if strings.TrimSpace(cfg.addr) == "" {
			continue
		}
		key := fmt.Sprintf("%t:%s", cfg.implicitTLS, cfg.addr)
		if seen[key] {
			return fmt.Errorf("duplicate mailbox SMTP listener: %s", cfg.addr)
		}
		seen[key] = true
		if cfg.implicitTLS && tlsConfig == nil {
			return fmt.Errorf("%s listener %s requires TLS_CERT_FILE and TLS_KEY_FILE", cfg.name, cfg.addr)
		}

		server := newSMTPServer(ctx, cfg, tlsConfig)
		var (
			ln  net.Listener
			err error
		)
		if cfg.implicitTLS {
			ln, err = tls.Listen("tcp", cfg.addr, tlsConfig.Clone())
		} else {
			ln, err = netListen("tcp", cfg.addr)
		}
		if err != nil {
			return fmt.Errorf("listen on %s %s: %w", cfg.name, cfg.addr, err)
		}
		log.Info("Mailbox %s listening on %s", cfg.name, cfg.addr)
		go serveSMTP(ctx, server, ln, cfg)
	}
	return nil
}

func newSMTPServer(ctx context.Context, cfg smtpListenerConfig, tlsConfig *tls.Config) *smtp.Server {
	server := smtp.NewServer(&smtpBackend{ctx: ctx, requireAuth: cfg.requireAuth})
	server.Addr = cfg.addr
	server.Domain = setting.MailboxServer.Hostname
	server.MaxRecipients = setting.MailboxServer.MaxRecipients
	server.MaxMessageBytes = setting.MailboxServer.MaxMessageSize
	server.MaxLineLength = smtpMaxLineLength
	server.ReadTimeout = smtpReadTimeout
	server.WriteTimeout = smtpWriteTimeout
	server.AllowInsecureAuth = setting.MailboxServer.AllowInsecureAuth
	server.ErrorLog = smtpErrorLog{name: cfg.name}
	// STARTTLS is only advertised on the cleartext listeners; the implicit-TLS
	// listener is already wrapped by its own tls.Listener.
	if !cfg.implicitTLS {
		server.TLSConfig = tlsConfig
	}
	return server
}

func serveSMTP(ctx context.Context, server *smtp.Server, ln net.Listener, cfg smtpListenerConfig) {
	go func() {
		<-ctx.Done()
		_ = server.Close()
	}()
	if err := server.Serve(ln); err != nil && ctx.Err() == nil {
		log.Error("Mailbox %s server stopped: %v", cfg.name, err)
	}
}

// smtpErrorLog routes go-smtp's internal errors into the Gitea log.
type smtpErrorLog struct {
	name string
}

func (l smtpErrorLog) Printf(format string, v ...any) {
	log.Warn("Mailbox %s: "+format, append([]any{l.name}, v...)...)
}

func (l smtpErrorLog) Println(v ...any) {
	log.Warn("Mailbox %s: %s", l.name, fmt.Sprintln(v...))
}

func (b *smtpBackend) NewSession(c *smtp.Conn) (smtp.Session, error) {
	return &smtpSession{ctx: b.ctx, conn: c, requireAuth: b.requireAuth}, nil
}

func (s *smtpSession) AuthMechanisms() []string {
	return []string{sasl.Plain, sasl.Login}
}

func (s *smtpSession) Auth(mech string) (sasl.Server, error) {
	authenticate := func(username, password string) error {
		user, err := Authenticate(s.ctx, username, password)
		if err != nil {
			log.Warn("Mailbox SMTP authentication failed for %q from %s: %v", username, s.remoteAddr(), err)
			return smtp.ErrAuthFailed
		}
		s.user = user
		return nil
	}
	switch mech {
	case sasl.Plain:
		return sasl.NewPlainServer(func(identity, username, password string) error {
			// An authorization identity that differs from the authentication
			// identity would be impersonation.
			if identity != "" && identity != username {
				return smtp.ErrAuthFailed
			}
			return authenticate(username, password)
		}), nil
	case sasl.Login:
		return newLoginServer(authenticate), nil
	default:
		return nil, smtp.ErrAuthUnknownMechanism
	}
}

func (s *smtpSession) Mail(from string, _ *smtp.MailOptions) error {
	if s.requireAuth && s.user == nil {
		return smtp.ErrAuthRequired
	}
	if s.user != nil && from != "" && !SenderAllowed(s.ctx, s.user, from) {
		return &smtp.SMTPError{Code: 553, EnhancedCode: smtp.EnhancedCode{5, 7, 1}, Message: "Sender address not owned by authenticated user"}
	}
	s.mailFrom = from
	s.recipients = nil
	return nil
}

func (s *smtpSession) Rcpt(to string, _ *smtp.RcptOptions) error {
	allowRelay := s.user != nil && setting.MailboxServer.RelayEnabled
	if err := CanAcceptRecipient(s.ctx, to, allowRelay); err != nil {
		return &smtp.SMTPError{Code: 550, EnhancedCode: smtp.EnhancedCode{5, 1, 1}, Message: "Recipient rejected"}
	}
	s.recipients = append(s.recipients, to)
	return nil
}

func (s *smtpSession) Data(r io.Reader) error {
	if len(s.recipients) == 0 {
		return &smtp.SMTPError{Code: 503, EnhancedCode: smtp.EnhancedCode{5, 5, 1}, Message: "Need RCPT TO before DATA"}
	}
	raw, err := io.ReadAll(r)
	if err != nil {
		// go-smtp reports the size overrun itself; anything else is a read failure.
		return err
	}

	remote := remoteIP(s.remoteAddr())
	_, tlsActive := s.conn.TLSConnectionState()
	deliveryOptions := DeliveryOptions{AllowRelay: s.user != nil && setting.MailboxServer.RelayEnabled}
	if s.user != nil {
		deliveryOptions.SenderID = s.user.ID
	}

	if s.user == nil {
		auth := authenticateIncoming(s.ctx, remote, s.conn.Hostname(), s.mailFrom, raw)
		logInboundAuthentication(auth.Action, remote, s.mailFrom)
		switch auth.Action {
		case inboundAuthDefer:
			return &smtp.SMTPError{Code: 451, EnhancedCode: smtp.EnhancedCode{4, 7, 5}, Message: "Message authentication temporarily unavailable"}
		case inboundAuthReject:
			return &smtp.SMTPError{Code: 550, EnhancedCode: smtp.EnhancedCode{5, 7, 1}, Message: "Message rejected by DMARC policy"}
		case inboundAuthQuarantine:
			deliveryOptions.SkipHandlers = true
			if setting.MailboxServer.DMARCQuarantineJunk {
				deliveryOptions.LocalFolder = mailbox_model.FolderJunk
			}
		}
		raw = prependMessageHeader(raw, auth.Header)
		raw = addReceivedHeader(raw, s.remoteAddr(), tlsActive)
	} else {
		if err := validateMessageFrom(s.ctx, s.user, raw); err != nil {
			log.Warn("Mailbox SMTP rejected unauthorized RFC5322.From for user %d: %v", s.user.ID, err)
			return &smtp.SMTPError{Code: 553, EnhancedCode: smtp.EnhancedCode{5, 7, 1}, Message: "RFC5322.From address is not owned by authenticated user"}
		}
		raw = addReceivedHeader(raw, s.remoteAddr(), tlsActive)
		if raw, err = SignOutboundDKIM(raw); err != nil {
			log.Error("Mailbox SMTP DKIM signing failed for authenticated user %d: %v", s.user.ID, err)
			return &smtp.SMTPError{Code: 451, EnhancedCode: smtp.EnhancedCode{4, 7, 0}, Message: "Temporary message signing failure"}
		}
	}

	if _, err := DeliverRawWithOptions(s.ctx, s.mailFrom, s.recipients, raw, deliveryOptions); err != nil {
		log.Error("Mailbox SMTP delivery failed from %q to %v: %v", s.mailFrom, s.recipients, err)
		if errors.Is(err, ErrQuotaExceeded) {
			return &smtp.SMTPError{Code: 552, EnhancedCode: smtp.EnhancedCode{5, 2, 2}, Message: "Mailbox quota exceeded"}
		}
		return &smtp.SMTPError{Code: 451, EnhancedCode: smtp.EnhancedCode{4, 3, 0}, Message: "Temporary delivery failure"}
	}
	return nil
}

func (s *smtpSession) Reset() {
	s.mailFrom = ""
	s.recipients = nil
}

func (s *smtpSession) Logout() error {
	s.Reset()
	s.user = nil
	return nil
}

func (s *smtpSession) remoteAddr() net.Addr {
	if s.conn == nil || s.conn.Conn() == nil {
		return nil
	}
	return s.conn.Conn().RemoteAddr()
}

// loginServer implements the non-standard but widely deployed AUTH LOGIN
// exchange, which go-sasl only provides a client for.
type loginServer struct {
	authenticate func(username, password string) error
	username     string
	hasUsername  bool
}

func newLoginServer(authenticate func(username, password string) error) sasl.Server {
	return &loginServer{authenticate: authenticate}
}

func (s *loginServer) Next(response []byte) ([]byte, bool, error) {
	if response == nil {
		return []byte("Username:"), false, nil
	}
	if !s.hasUsername {
		s.username, s.hasUsername = string(response), true
		return []byte("Password:"), false, nil
	}
	if err := s.authenticate(s.username, string(response)); err != nil {
		return nil, false, err
	}
	return nil, true, nil
}

func remoteIP(remote net.Addr) net.IP {
	if remote == nil {
		return nil
	}
	if tcp, ok := remote.(*net.TCPAddr); ok {
		return tcp.IP
	}
	host, _, err := net.SplitHostPort(remote.String())
	if err != nil {
		return net.ParseIP(remote.String())
	}
	return net.ParseIP(host)
}

func addReceivedHeader(raw []byte, remote net.Addr, tlsActive bool) []byte {
	remoteText := "unknown"
	if remote != nil {
		remoteText = remote.String()
		if host, _, err := net.SplitHostPort(remoteText); err == nil {
			remoteText = host
		}
	}
	with := "ESMTP"
	if tlsActive {
		with = "ESMTPS"
	}
	header := fmt.Sprintf("Received: from [%s] by %s with %s; %s\r\n", strings.NewReplacer("\r", "", "\n", "").Replace(remoteText), setting.MailboxServer.Hostname, with, time.Now().Format(time.RFC1123Z))
	out := make([]byte, 0, len(header)+len(raw))
	out = append(out, header...)
	out = append(out, raw...)
	return out
}
