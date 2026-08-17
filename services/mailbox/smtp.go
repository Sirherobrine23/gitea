// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package mailbox

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/mail"
	"strings"
	"time"

	user_model "gitea.dev/models/user"
	"gitea.dev/modules/log"
	"gitea.dev/modules/setting"
)

const smtpSessionTimeout = 10 * time.Minute

type smtpListenerConfig struct {
	addr        string
	requireAuth bool
	implicitTLS bool
	name        string
}

type smtpSession struct {
	ctx         context.Context
	conn        net.Conn
	rw          *bufio.ReadWriter
	tlsConfig   *tls.Config
	requireAuth bool
	implicitTLS bool
	tlsActive   bool
	helo        bool
	user        *user_model.User
	mailFrom    string
	mailSet     bool
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

		var (
			ln  net.Listener
			err error
		)
		if cfg.implicitTLS {
			ln, err = tls.Listen("tcp", cfg.addr, tlsConfig)
		} else {
			ln, err = net.Listen("tcp", cfg.addr)
		}
		if err != nil {
			return fmt.Errorf("listen on %s %s: %w", cfg.name, cfg.addr, err)
		}
		log.Info("Mailbox %s listening on %s", cfg.name, cfg.addr)
		go serveSMTPListener(ctx, ln, cfg, tlsConfig)
	}
	return nil
}

func serveSMTPListener(ctx context.Context, ln net.Listener, cfg smtpListenerConfig, tlsConfig *tls.Config) {
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() == nil {
				log.Error("Mailbox %s accept error: %v", cfg.name, err)
			}
			return
		}
		go (&smtpSession{
			ctx:         ctx,
			conn:        conn,
			tlsConfig:   tlsConfig,
			requireAuth: cfg.requireAuth,
			implicitTLS: cfg.implicitTLS,
			tlsActive:   cfg.implicitTLS,
		}).serve()
	}
}

func (s *smtpSession) serve() {
	defer s.conn.Close()
	s.rw = bufio.NewReadWriter(bufio.NewReader(s.conn), bufio.NewWriter(s.conn))
	if err := s.reply(220, "%s ESMTP Gitea Mailbox", setting.MailboxServer.Hostname); err != nil {
		return
	}
	for {
		if err := s.conn.SetDeadline(time.Now().Add(smtpSessionTimeout)); err != nil {
			return
		}
		line, err := s.readCommandLine()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				_ = s.reply(421, "4.4.2 Connection error")
			}
			return
		}
		verb, arg, _ := strings.Cut(strings.TrimSpace(line), " ")
		verb = strings.ToUpper(verb)
		switch verb {
		case "EHLO":
			s.resetTransaction()
			s.helo = true
			if err := s.ehlo(arg); err != nil {
				return
			}
		case "HELO":
			s.resetTransaction()
			s.helo = true
			if err := s.reply(250, "%s", setting.MailboxServer.Hostname); err != nil {
				return
			}
		case "NOOP":
			_ = s.reply(250, "2.0.0 OK")
		case "RSET":
			s.resetTransaction()
			_ = s.reply(250, "2.0.0 OK")
		case "QUIT":
			_ = s.reply(221, "2.0.0 Bye")
			return
		case "STARTTLS":
			if !s.startTLS() {
				return
			}
		case "AUTH":
			s.auth(arg)
		case "MAIL":
			s.mail(arg)
		case "RCPT":
			s.rcpt(arg)
		case "DATA":
			s.data()
		case "VRFY", "EXPN":
			_ = s.reply(252, "2.5.2 Cannot verify user")
		case "":
			_ = s.reply(500, "5.5.2 Empty command")
		default:
			_ = s.reply(502, "5.5.1 Command not implemented")
		}
	}
}

func (s *smtpSession) readCommandLine() (string, error) {
	line, err := s.rw.ReadString('\n')
	if len(line) > 65536 {
		return "", errors.New("SMTP command line too long")
	}
	return strings.TrimRight(line, "\r\n"), err
}

func (s *smtpSession) reply(code int, format string, args ...any) error {
	if _, err := fmt.Fprintf(s.rw, "%d %s\r\n", code, fmt.Sprintf(format, args...)); err != nil {
		return err
	}
	return s.rw.Flush()
}

func (s *smtpSession) ehlo(_ string) error {
	caps := []string{
		setting.MailboxServer.Hostname,
		"PIPELINING",
		"8BITMIME",
	}
	if setting.MailboxServer.MaxMessageSize > 0 {
		caps = append(caps, fmt.Sprintf("SIZE %d", setting.MailboxServer.MaxMessageSize))
	} else {
		caps = append(caps, "SIZE")
	}
	if s.tlsConfig != nil && !s.tlsActive && !s.implicitTLS {
		caps = append(caps, "STARTTLS")
	}
	if s.tlsActive || setting.MailboxServer.AllowInsecureAuth {
		caps = append(caps, "AUTH PLAIN LOGIN")
	}
	for i, cap := range caps {
		sep := "-"
		if i == len(caps)-1 {
			sep = " "
		}
		if _, err := fmt.Fprintf(s.rw, "250%s%s\r\n", sep, cap); err != nil {
			return err
		}
	}
	return s.rw.Flush()
}

func (s *smtpSession) startTLS() bool {
	if s.tlsActive || s.implicitTLS {
		_ = s.reply(503, "5.5.1 TLS already active")
		return true
	}
	if s.tlsConfig == nil {
		_ = s.reply(454, "4.7.0 TLS not available")
		return true
	}
	if err := s.reply(220, "2.0.0 Ready to start TLS"); err != nil {
		return false
	}
	tlsConn := tls.Server(s.conn, s.tlsConfig.Clone())
	if err := tlsConn.HandshakeContext(s.ctx); err != nil {
		return false
	}
	s.conn = tlsConn
	s.rw = bufio.NewReadWriter(bufio.NewReader(tlsConn), bufio.NewWriter(tlsConn))
	s.tlsActive = true
	s.helo = false
	s.user = nil
	s.resetTransaction()
	return true
}

func (s *smtpSession) auth(arg string) {
	if !s.helo {
		_ = s.reply(503, "5.5.1 EHLO/HELO first")
		return
	}
	if s.user != nil {
		_ = s.reply(503, "5.5.0 Already authenticated")
		return
	}
	if !s.tlsActive && !setting.MailboxServer.AllowInsecureAuth {
		_ = s.reply(538, "5.7.11 Encryption required for requested authentication mechanism")
		return
	}
	fields := strings.Fields(arg)
	if len(fields) == 0 {
		_ = s.reply(501, "5.5.4 Missing authentication mechanism")
		return
	}
	var username, password string
	var err error
	switch strings.ToUpper(fields[0]) {
	case "PLAIN":
		payload := ""
		if len(fields) > 1 {
			payload = fields[1]
		} else {
			if err := s.reply(334, ""); err != nil {
				return
			}
			payload, err = s.readCommandLine()
			if err != nil {
				return
			}
		}
		username, password, err = decodePlainAuth(payload)
	case "LOGIN":
		if len(fields) > 1 {
			username, err = decodeBase64String(fields[1])
		} else {
			if err = s.reply(334, base64.StdEncoding.EncodeToString([]byte("Username:"))); err != nil {
				return
			}
			var line string
			line, err = s.readCommandLine()
			if err == nil {
				username, err = decodeBase64String(line)
			}
		}
		if err == nil {
			if err = s.reply(334, base64.StdEncoding.EncodeToString([]byte("Password:"))); err != nil {
				return
			}
			var line string
			line, err = s.readCommandLine()
			if err == nil {
				password, err = decodeBase64String(line)
			}
		}
	default:
		_ = s.reply(504, "5.5.4 Unsupported authentication mechanism")
		return
	}
	if err != nil {
		_ = s.reply(501, "5.5.2 Invalid authentication payload")
		return
	}
	user, err := Authenticate(s.ctx, username, password)
	if err != nil {
		log.Warn("Mailbox SMTP authentication failed for %q from %s: %v", username, s.conn.RemoteAddr(), err)
		_ = s.reply(535, "5.7.8 Authentication credentials invalid")
		return
	}
	s.user = user
	_ = s.reply(235, "2.7.0 Authentication successful")
}

func decodePlainAuth(payload string) (string, string, error) {
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(payload))
	if err != nil {
		return "", "", err
	}
	parts := bytes.Split(decoded, []byte{0})
	if len(parts) != 3 || len(parts[1]) == 0 {
		return "", "", errors.New("invalid AUTH PLAIN payload")
	}
	if len(parts[0]) != 0 && !bytes.Equal(parts[0], parts[1]) {
		return "", "", errors.New("authorization identity is not supported")
	}
	return string(parts[1]), string(parts[2]), nil
}

func decodeBase64String(v string) (string, error) {
	b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(v))
	return string(b), err
}

func (s *smtpSession) mail(arg string) {
	if !s.helo {
		_ = s.reply(503, "5.5.1 EHLO/HELO first")
		return
	}
	if s.requireAuth && s.user == nil {
		_ = s.reply(530, "5.7.0 Authentication required")
		return
	}
	if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(arg)), "FROM:") {
		_ = s.reply(501, "5.5.4 MAIL requires FROM")
		return
	}
	path, err := parseSMTPPath(strings.TrimSpace(arg[len("FROM:"):]), true)
	if err != nil {
		_ = s.reply(501, "5.1.7 Bad sender address syntax")
		return
	}
	if s.user != nil && path != "" && !SenderAllowed(s.ctx, s.user, path) {
		_ = s.reply(553, "5.7.1 Sender address not owned by authenticated user")
		return
	}
	s.mailFrom = path
	s.mailSet = true
	s.recipients = nil
	_ = s.reply(250, "2.1.0 Sender OK")
}

func (s *smtpSession) rcpt(arg string) {
	if !s.mailSet {
		_ = s.reply(503, "5.5.1 Need MAIL FROM before RCPT TO")
		return
	}
	if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(arg)), "TO:") {
		_ = s.reply(501, "5.5.4 RCPT requires TO")
		return
	}
	if len(s.recipients) >= setting.MailboxServer.MaxRecipients {
		_ = s.reply(452, "4.5.3 Too many recipients")
		return
	}
	path, err := parseSMTPPath(strings.TrimSpace(arg[len("TO:"):]), false)
	if err != nil || path == "" {
		_ = s.reply(501, "5.1.3 Bad recipient address syntax")
		return
	}
	allowRelay := s.user != nil && setting.MailboxServer.RelayEnabled
	if err := CanAcceptRecipient(s.ctx, path, allowRelay); err != nil {
		_ = s.reply(550, "5.1.1 Recipient rejected")
		return
	}
	s.recipients = append(s.recipients, path)
	_ = s.reply(250, "2.1.5 Recipient OK")
}

func parseSMTPPath(v string, allowEmpty bool) (string, error) {
	v = strings.TrimSpace(v)
	if !strings.HasPrefix(v, "<") {
		return "", errors.New("SMTP path must start with <")
	}
	end := strings.IndexByte(v, '>')
	if end < 0 {
		return "", errors.New("SMTP path must end with >")
	}
	path := v[1:end]
	params := strings.TrimSpace(v[end+1:])
	if strings.ContainsAny(params, "\r\n") {
		return "", errors.New("invalid ESMTP parameters")
	}
	if path == "" && allowEmpty {
		return "", nil
	}
	if path == "" {
		return "", errors.New("empty SMTP path")
	}
	parsed, err := mail.ParseAddress(path)
	if err != nil || !strings.EqualFold(parsed.Address, path) {
		if err == nil {
			err = errors.New("SMTP path must contain only an addr-spec")
		}
		return "", err
	}
	return parsed.Address, nil
}

func (s *smtpSession) data() {
	if len(s.recipients) == 0 {
		_ = s.reply(503, "5.5.1 Need RCPT TO before DATA")
		return
	}
	if err := s.reply(354, "End data with <CR><LF>.<CR><LF>"); err != nil {
		return
	}
	var buf bytes.Buffer
	tooLarge := false
	for {
		if err := s.conn.SetDeadline(time.Now().Add(smtpSessionTimeout)); err != nil {
			return
		}
		line, err := s.rw.ReadString('\n')
		if err != nil {
			return
		}
		trimmed := strings.TrimRight(line, "\r\n")
		if trimmed == "." {
			break
		}
		if strings.HasPrefix(trimmed, "..") {
			trimmed = trimmed[1:]
		}
		if !tooLarge {
			buf.WriteString(trimmed)
			buf.WriteString("\r\n")
			if setting.MailboxServer.MaxMessageSize > 0 && int64(buf.Len()) > setting.MailboxServer.MaxMessageSize {
				tooLarge = true
			}
		}
	}
	if tooLarge {
		_ = s.reply(552, "5.3.4 Message size exceeds fixed maximum message size")
		s.resetTransaction()
		return
	}
	allowRelay := s.user != nil && setting.MailboxServer.RelayEnabled
	raw := addReceivedHeader(buf.Bytes(), s.conn.RemoteAddr(), s.tlsActive)
	if _, err := DeliverRaw(s.ctx, s.mailFrom, s.recipients, raw, allowRelay); err != nil {
		log.Error("Mailbox SMTP delivery failed from %q to %v: %v", s.mailFrom, s.recipients, err)
		if errors.Is(err, ErrQuotaExceeded) {
			_ = s.reply(552, "5.2.2 Mailbox quota exceeded")
		} else {
			_ = s.reply(451, "4.3.0 Temporary delivery failure")
		}
		s.resetTransaction()
		return
	}
	_ = s.reply(250, "2.0.0 Message accepted for delivery")
	s.resetTransaction()
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

func (s *smtpSession) resetTransaction() {
	s.mailFrom = ""
	s.mailSet = false
	s.recipients = nil
}
