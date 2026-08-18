// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package mailbox

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"

	user_model "gitea.dev/models/user"
	"gitea.dev/modules/setting"
	"gitea.dev/modules/test"

	"github.com/emersion/go-sasl"
	"github.com/emersion/go-smtp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoginServerExchange(t *testing.T) {
	var gotUser, gotPass string
	server := newLoginServer(func(username, password string) error {
		gotUser, gotPass = username, password
		return nil
	})

	// AUTH LOGIN with no initial response prompts for each field in turn.
	challenge, done, err := server.Next(nil)
	require.NoError(t, err)
	assert.False(t, done)
	assert.Equal(t, "Username:", string(challenge))

	challenge, done, err = server.Next([]byte("alice"))
	require.NoError(t, err)
	assert.False(t, done)
	assert.Equal(t, "Password:", string(challenge))

	_, done, err = server.Next([]byte("secret"))
	require.NoError(t, err)
	assert.True(t, done)
	assert.Equal(t, "alice", gotUser)
	assert.Equal(t, "secret", gotPass)
}

func TestLoginServerRejectsBadCredentials(t *testing.T) {
	server := newLoginServer(func(string, string) error { return smtp.ErrAuthFailed })
	_, _, err := server.Next(nil)
	require.NoError(t, err)
	_, _, err = server.Next([]byte("alice"))
	require.NoError(t, err)
	_, done, err := server.Next([]byte("wrong"))
	assert.Error(t, err)
	assert.False(t, done)
}

func TestRemoteIP(t *testing.T) {
	assert.Equal(t, "192.0.2.1", remoteIP(&net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 25}).String())
	assert.Nil(t, remoteIP(nil))
}

func TestAddReceivedHeader(t *testing.T) {
	t.Cleanup(test.MockVariableValue(&setting.MailboxServer.Hostname, "mail.example.com"))
	raw := []byte("Subject: hi\r\n\r\nbody\r\n")

	header := string(addReceivedHeader(raw, &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 40000}, false))
	assert.True(t, strings.HasPrefix(header, "Received: from [192.0.2.1] by mail.example.com with ESMTP;"))
	assert.True(t, strings.HasSuffix(header, string(raw)), "the message must be preserved verbatim")

	// A TLS session is recorded as ESMTPS.
	assert.Contains(t, string(addReceivedHeader(raw, nil, true)), "with ESMTPS;")
	assert.Contains(t, string(addReceivedHeader(raw, nil, false)), "from [unknown]")
}

// startTestSMTP runs a real go-smtp server on a loopback port and returns its address.
func startTestSMTP(t *testing.T, requireAuth bool) string {
	t.Helper()
	t.Cleanup(test.MockVariableValue(&setting.MailboxServer.Hostname, "mail.example.com"))
	t.Cleanup(test.MockVariableValue(&setting.MailboxServer.AllowInsecureAuth, true))
	t.Cleanup(test.MockVariableValue(&setting.MailboxServer.MaxRecipients, 100))

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	cfg := smtpListenerConfig{addr: ln.Addr().String(), requireAuth: requireAuth, name: "SMTP test"}
	server := newSMTPServer(ctx, cfg, nil)
	go func() { _ = server.Serve(ln) }()
	t.Cleanup(func() {
		cancel()
		_ = server.Close()
	})
	return ln.Addr().String()
}

func TestSMTPSubmissionRequiresAuth(t *testing.T) {
	addr := startTestSMTP(t, true)

	client, err := smtp.Dial(addr)
	require.NoError(t, err)
	defer client.Close()
	require.NoError(t, client.Hello("client.example.net"))

	// A submission listener must refuse a return path before authentication.
	err = client.Mail("alice@example.com", nil)
	require.Error(t, err)
	var smtpErr *smtp.SMTPError
	require.ErrorAs(t, err, &smtpErr)
	assert.Equal(t, 502, smtpErr.Code)
}

func TestSMTPAuthRejectsInvalidCredentials(t *testing.T) {
	t.Cleanup(test.MockVariableValue(&signInFunc, func(context.Context, string, string) (*user_model.User, error) {
		return nil, errors.New("invalid credentials")
	}))
	addr := startTestSMTP(t, true)

	client, err := smtp.Dial(addr)
	require.NoError(t, err)
	defer client.Close()
	require.NoError(t, client.Hello("client.example.net"))

	// Both mechanisms our sessions advertise must reject a bad password.
	assert.Error(t, client.Auth(sasl.NewPlainClient("", "alice", "wrong")))
	assert.Error(t, client.Auth(sasl.NewLoginClient("alice", "wrong")))
}

func TestSMTPAdvertisesConfiguredLimits(t *testing.T) {
	t.Cleanup(test.MockVariableValue(&setting.MailboxServer.MaxMessageSize, int64(1234)))
	addr := startTestSMTP(t, false)

	client, err := smtp.Dial(addr)
	require.NoError(t, err)
	defer client.Close()
	require.NoError(t, client.Hello("client.example.net"))

	// SIZE is what tells a sender to give up before transferring a huge message.
	ok, size := client.Extension("SIZE")
	assert.True(t, ok)
	assert.Equal(t, "1234", size)

	hasAuth, _ := client.Extension("AUTH")
	assert.True(t, hasAuth, "AUTH is advertised once insecure auth is permitted")
}
