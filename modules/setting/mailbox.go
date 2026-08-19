// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package setting

import (
	"strings"
	"time"
)

// Outbound delivery modes for remote recipients.
const (
	// OutboundModeDirect resolves the recipient domain's MX records and delivers
	// straight to it, so no [mailer] transport is required.
	OutboundModeDirect = "direct"
	// OutboundModeRelay hands remote recipients to the configured [mailer]
	// transport, which is the right choice behind a smarthost.
	OutboundModeRelay = "relay"
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

	PostmasterUser string `ini:"POSTMASTER_USER"`
	CatchAllUser   string `ini:"CATCH_ALL_USER"`

	OutboundMode        string        `ini:"OUTBOUND_MODE"`
	OutboundHelo        string        `ini:"OUTBOUND_HELO"`
	OutboundRequireTLS  bool          `ini:"OUTBOUND_REQUIRE_TLS"`
	OutboundRetryMaxAge time.Duration `ini:"OUTBOUND_RETRY_MAX_AGE"`
	OutboundRetryEvery  time.Duration `ini:"OUTBOUND_RETRY_EVERY"`
	OutboundConcurrency int           `ini:"OUTBOUND_CONCURRENCY"`

	DKIMEnabled                bool   `ini:"DKIM_ENABLED"`
	DKIMDomain                 string `ini:"DKIM_DOMAIN"`
	DKIMSelector               string `ini:"DKIM_SELECTOR"`
	DKIMPrivateKeyFile         string `ini:"DKIM_PRIVATE_KEY_FILE"`
	DKIMHeaderCanonicalization string `ini:"DKIM_HEADER_CANONICALIZATION"`
	DKIMBodyCanonicalization   string `ini:"DKIM_BODY_CANONICALIZATION"`

	VerifyDKIM           bool `ini:"VERIFY_DKIM"`
	VerifySPF            bool `ini:"VERIFY_SPF"`
	VerifyDMARC          bool `ini:"VERIFY_DMARC"`
	DMARCEnforce         bool `ini:"DMARC_ENFORCE"`
	DMARCDeferOnTempFail bool `ini:"DMARC_DEFER_ON_TEMPFAIL"`
	DMARCQuarantineJunk  bool `ini:"DMARC_QUARANTINE_TO_JUNK"`
}{
	WebEnabled:                 true,
	SMTPListen:                 ":25",
	SMTPSubmissionListen:       ":587",
	SMTPSListen:                "",
	IMAPListen:                 ":143",
	IMAPSListen:                "",
	MaxMessageSize:             25 * 1024 * 1024,
	MaxRecipients:              100,
	RelayEnabled:               true,
	OutboundMode:               OutboundModeDirect,
	OutboundRetryMaxAge:        72 * time.Hour,
	OutboundRetryEvery:         5 * time.Minute,
	OutboundConcurrency:        4,
	DKIMSelector:               "gitea",
	DKIMHeaderCanonicalization: "relaxed",
	DKIMBodyCanonicalization:   "relaxed",
	VerifyDKIM:                 true,
	VerifySPF:                  true,
	VerifyDMARC:                true,
	DMARCEnforce:               true,
	DMARCDeferOnTempFail:       true,
	DMARCQuarantineJunk:        true,
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
	sec.Key("POSTMASTER_USER").MustString("")
	sec.Key("CATCH_ALL_USER").MustString("")
	sec.Key("OUTBOUND_MODE").MustString(OutboundModeDirect)
	sec.Key("OUTBOUND_HELO").MustString("")
	sec.Key("OUTBOUND_REQUIRE_TLS").MustBool(false)
	sec.Key("OUTBOUND_RETRY_MAX_AGE").MustDuration(72 * time.Hour)
	sec.Key("OUTBOUND_RETRY_EVERY").MustDuration(5 * time.Minute)
	sec.Key("OUTBOUND_CONCURRENCY").MustInt(4)
	sec.Key("DKIM_ENABLED").MustBool(false)
	sec.Key("DKIM_DOMAIN").MustString("")
	sec.Key("DKIM_SELECTOR").MustString("gitea")
	sec.Key("DKIM_PRIVATE_KEY_FILE").MustString("")
	sec.Key("DKIM_HEADER_CANONICALIZATION").MustString("relaxed")
	sec.Key("DKIM_BODY_CANONICALIZATION").MustString("relaxed")
	sec.Key("VERIFY_DKIM").MustBool(true)
	sec.Key("VERIFY_SPF").MustBool(true)
	sec.Key("VERIFY_DMARC").MustBool(true)
	sec.Key("DMARC_ENFORCE").MustBool(true)
	sec.Key("DMARC_DEFER_ON_TEMPFAIL").MustBool(true)
	sec.Key("DMARC_QUARANTINE_TO_JUNK").MustBool(true)

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
	MailboxServer.OutboundMode = strings.ToLower(strings.TrimSpace(MailboxServer.OutboundMode))
	MailboxServer.OutboundHelo = strings.TrimSpace(MailboxServer.OutboundHelo)
	MailboxServer.PostmasterUser = strings.ToLower(strings.TrimSpace(MailboxServer.PostmasterUser))
	MailboxServer.CatchAllUser = strings.ToLower(strings.TrimSpace(MailboxServer.CatchAllUser))
	MailboxServer.DKIMDomain = strings.ToLower(strings.TrimSpace(MailboxServer.DKIMDomain))
	MailboxServer.DKIMSelector = strings.TrimSpace(MailboxServer.DKIMSelector)
	MailboxServer.DKIMPrivateKeyFile = strings.TrimSpace(MailboxServer.DKIMPrivateKeyFile)
	MailboxServer.DKIMHeaderCanonicalization = strings.ToLower(strings.TrimSpace(MailboxServer.DKIMHeaderCanonicalization))
	MailboxServer.DKIMBodyCanonicalization = strings.ToLower(strings.TrimSpace(MailboxServer.DKIMBodyCanonicalization))
	if MailboxServer.Hostname == "" {
		MailboxServer.Hostname = MailboxServer.Domain
	}
	if MailboxServer.DKIMDomain == "" {
		MailboxServer.DKIMDomain = MailboxServer.Domain
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
	if MailboxServer.OutboundMode != OutboundModeRelay {
		MailboxServer.OutboundMode = OutboundModeDirect
	}
	if MailboxServer.OutboundHelo == "" {
		MailboxServer.OutboundHelo = MailboxServer.Hostname
	}
	if MailboxServer.OutboundRetryEvery < time.Minute {
		MailboxServer.OutboundRetryEvery = time.Minute
	}
	if MailboxServer.OutboundRetryMaxAge <= 0 {
		MailboxServer.OutboundRetryMaxAge = 72 * time.Hour
	}
	if MailboxServer.OutboundConcurrency <= 0 {
		MailboxServer.OutboundConcurrency = 4
	}
}
