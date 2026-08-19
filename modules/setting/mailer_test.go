// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package setting

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadMailboxOnlyMailer(t *testing.T) {
	restore := func(service *Mailer, enabled bool, domain string) func() {
		return func() { MailService, MailboxServer.Enabled, MailboxServer.Domain = service, enabled, domain }
	}

	t.Run("MailboxOnly", func(t *testing.T) {
		t.Cleanup(restore(MailService, MailboxServer.Enabled, MailboxServer.Domain))
		MailService = nil
		MailboxServer.Enabled, MailboxServer.Domain = true, "git.example.com"

		loadMailboxOnlyMailer()

		// Mail composition reads MailService throughout, so it must exist even
		// when no [mailer] transport is configured.
		require.NotNil(t, MailService)
		assert.Equal(t, MailerProtocolMailbox, MailService.Protocol)
		assert.Equal(t, "gitea@git.example.com", MailService.FromEmail)
		assert.Contains(t, MailService.From, "gitea@git.example.com")
		assert.NotNil(t, MailService.OverrideHeader)
	})

	t.Run("DoesNotOverrideConfiguredMailer", func(t *testing.T) {
		t.Cleanup(restore(MailService, MailboxServer.Enabled, MailboxServer.Domain))
		configured := &Mailer{Protocol: "smtp", FromEmail: "real@example.com"}
		MailService = configured
		MailboxServer.Enabled = true

		loadMailboxOnlyMailer()

		assert.Same(t, configured, MailService, "a configured [mailer] must win")
		assert.Equal(t, "smtp", MailService.Protocol)
	})

	t.Run("MailboxDisabled", func(t *testing.T) {
		t.Cleanup(restore(MailService, MailboxServer.Enabled, MailboxServer.Domain))
		MailService = nil
		MailboxServer.Enabled = false

		loadMailboxOnlyMailer()

		assert.Nil(t, MailService, "no mail service without either transport")
	})
}
