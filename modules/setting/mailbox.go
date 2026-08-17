// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package setting

import (
	"strings"
)

// MailboxServer configures the integrated mailbox, SMTP and IMAP services.
var MailboxServer = struct {
	Enabled bool
	Domain  string

	WebEnabled bool `ini:"WEB_ENABLED"`

	SMTPListen           string `ini:"SMTP_LISTEN"`
	SMTPSubmissionListen string `ini:"SMTP_SUBMISSION_LISTEN"`
	SMTPSListen          string `ini:"SMTPS_LISTEN"`
	IMAPListen           string `ini:"IMAP_LISTEN"`
	IMAPSListen          string `ini:"IMAPS_LISTEN"`

	TLSCertFile string `ini:"TLS_CERT_FILE"`
	TLSKeyFile  string `ini:"TLS_KEY_FILE"`

	AllowInsecureAuth bool   `ini:"ALLOW_INSECURE_AUTH"`
	RelayEnabled      bool   `ini:"RELAY_ENABLED"`
	MaxMessageSize    int64  `ini:"MAX_MESSAGE_SIZE"`
	MaxRecipients     int    `ini:"MAX_RECIPIENTS"`
	DefaultQuota      int64  `ini:"DEFAULT_QUOTA"`
	Hostname          string `ini:"HOSTNAME"`
}{
	WebEnabled:           true,
	SMTPListen:           ":25",
	SMTPSubmissionListen: ":587",
	SMTPSListen:          "",
	IMAPListen:           ":143",
	IMAPSListen:          "",
	MaxMessageSize:       25 * 1024 * 1024,
	MaxRecipients:        100,
	RelayEnabled:         true,
}

func loadMailboxServerFrom(rootCfg ConfigProvider) {
	sec := rootCfg.Section("mailbox")
	sec.Key("DOMAIN").MustString(Domain)
	sec.Key("WEB_ENABLED").MustBool(true)
	sec.Key("SMTP_LISTEN").MustString(":25")
	sec.Key("SMTP_SUBMISSION_LISTEN").MustString(":587")
	sec.Key("SMTPS_LISTEN").MustString("")
	sec.Key("IMAP_LISTEN").MustString(":143")
	sec.Key("IMAPS_LISTEN").MustString("")
	sec.Key("ALLOW_INSECURE_AUTH").MustBool(false)
	sec.Key("RELAY_ENABLED").MustBool(true)
	sec.Key("MAX_MESSAGE_SIZE").MustInt64(25 * 1024 * 1024)
	sec.Key("MAX_RECIPIENTS").MustInt(100)
	sec.Key("DEFAULT_QUOTA").MustInt64(0)
	sec.Key("HOSTNAME").MustString(Domain)

	mustMapSetting(rootCfg, "mailbox", &MailboxServer)
	MailboxServer.Domain = strings.ToLower(strings.TrimSpace(MailboxServer.Domain))
	MailboxServer.Hostname = strings.TrimSpace(MailboxServer.Hostname)
	MailboxServer.SMTPListen = strings.TrimSpace(MailboxServer.SMTPListen)
	MailboxServer.SMTPSubmissionListen = strings.TrimSpace(MailboxServer.SMTPSubmissionListen)
	MailboxServer.SMTPSListen = strings.TrimSpace(MailboxServer.SMTPSListen)
	MailboxServer.IMAPListen = strings.TrimSpace(MailboxServer.IMAPListen)
	MailboxServer.IMAPSListen = strings.TrimSpace(MailboxServer.IMAPSListen)
	MailboxServer.TLSCertFile = strings.TrimSpace(MailboxServer.TLSCertFile)
	MailboxServer.TLSKeyFile = strings.TrimSpace(MailboxServer.TLSKeyFile)
	if MailboxServer.Hostname == "" {
		MailboxServer.Hostname = MailboxServer.Domain
	}
	if MailboxServer.MaxMessageSize < 0 {
		MailboxServer.MaxMessageSize = 0
	}
	if MailboxServer.DefaultQuota < 0 {
		MailboxServer.DefaultQuota = 0
	}
	if MailboxServer.MaxRecipients <= 0 {
		MailboxServer.MaxRecipients = 100
	}
}
