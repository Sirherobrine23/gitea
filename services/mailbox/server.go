// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package mailbox

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"

	"gitea.dev/modules/log"
	"gitea.dev/modules/setting"
)

func loadTLSConfig() (*tls.Config, error) {
	if setting.MailboxServer.TLSCertFile == "" && setting.MailboxServer.TLSKeyFile == "" {
		return nil, nil
	}
	if setting.MailboxServer.TLSCertFile == "" || setting.MailboxServer.TLSKeyFile == "" {
		return nil, fmt.Errorf("both [mailbox] TLS_CERT_FILE and TLS_KEY_FILE must be configured together")
	}
	cert, err := tls.LoadX509KeyPair(setting.MailboxServer.TLSCertFile, setting.MailboxServer.TLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load mailbox TLS certificate: %w", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// Init starts the integrated SMTP and IMAP services. The listeners are only
// created when [mailbox] ENABLED is true.
// netListen is a variable so listener creation can be unit-tested without
// binding privileged ports.
var netListen = func(network, address string) (net.Listener, error) {
	return net.Listen(network, address)
}

func Init(ctx context.Context) error {
	if !setting.MailboxServer.Enabled {
		return nil
	}
	if Domain() == "" {
		return fmt.Errorf("[mailbox] DOMAIN must be configured when the mailbox server is enabled")
	}

	if err := initMessageAuthentication(); err != nil {
		return err
	}

	tlsConfig, err := loadTLSConfig()
	if err != nil {
		return err
	}
	if tlsConfig == nil && !setting.MailboxServer.AllowInsecureAuth &&
		(setting.MailboxServer.SMTPSubmissionListen != "" || setting.MailboxServer.IMAPListen != "") {
		return fmt.Errorf("mailbox authenticated listeners require TLS_CERT_FILE/TLS_KEY_FILE; configure TLS, disable SMTP_SUBMISSION_LISTEN/IMAP_LISTEN, or explicitly set ALLOW_INSECURE_AUTH")
	}
	if err := initSMTP(ctx, tlsConfig); err != nil {
		return err
	}
	if err := initIMAP(ctx, tlsConfig); err != nil {
		return err
	}
	if setting.MailboxServer.OutboundMode == setting.OutboundModeDirect {
		log.Info("Mailbox outbound delivery is direct-to-MX; [mailer] is not required")
		go RunOutboundQueue(ctx)
	}
	return nil
}
