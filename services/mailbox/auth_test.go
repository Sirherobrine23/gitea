// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package mailbox

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	"gitea.dev/modules/setting"
	"gitea.dev/modules/test"

	"github.com/emersion/go-msgauth/dkim"
	"github.com/emersion/go-msgauth/dmarc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testMessage = "From: Alice <alice@example.com>\r\n" +
	"To: Bob <bob@example.org>\r\n" +
	"Subject: hello\r\n" +
	"Date: Tue, 18 Aug 2026 10:00:00 +0000\r\n" +
	"Message-ID: <abc@example.com>\r\n" +
	"\r\n" +
	"body line one\r\n" +
	"body line two\r\n"

// writeRSAKey writes a PEM private key and returns its path plus the DKIM public
// key record value that a verifier would find in DNS.
func writeRSAKey(t *testing.T, pkcs8 bool) (string, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	var block *pem.Block
	if pkcs8 {
		der, err := x509.MarshalPKCS8PrivateKey(key)
		require.NoError(t, err)
		block = &pem.Block{Type: "PRIVATE KEY", Bytes: der}
	} else {
		block = &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}
	}
	path := filepath.Join(t.TempDir(), "dkim.key")
	require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(block), 0o600))

	pub, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	require.NoError(t, err)
	return path, "v=DKIM1; k=rsa; p=" + base64.StdEncoding.EncodeToString(pub)
}

// configureDKIM points the package at a key file and clears the cached signer so
// each test signs with its own key.
func configureDKIM(t *testing.T, keyFile string) {
	t.Helper()
	t.Cleanup(test.MockVariableValue(&setting.MailboxServer.DKIMEnabled, true))
	t.Cleanup(test.MockVariableValue(&setting.MailboxServer.DKIMDomain, "example.com"))
	t.Cleanup(test.MockVariableValue(&setting.MailboxServer.DKIMSelector, "gitea"))
	t.Cleanup(test.MockVariableValue(&setting.MailboxServer.DKIMPrivateKeyFile, keyFile))
	t.Cleanup(test.MockVariableValue(&setting.MailboxServer.DKIMHeaderCanonicalization, "relaxed"))
	t.Cleanup(test.MockVariableValue(&setting.MailboxServer.DKIMBodyCanonicalization, "relaxed"))

	outboundDKIMSignerMu.Lock()
	outboundDKIMSigner = nil
	outboundDKIMSignerMu.Unlock()
	t.Cleanup(func() {
		outboundDKIMSignerMu.Lock()
		outboundDKIMSigner = nil
		outboundDKIMSignerMu.Unlock()
	})
}

func TestSignOutboundDKIMVerifies(t *testing.T) {
	for _, pkcs8 := range []bool{false, true} {
		name := "PKCS1"
		if pkcs8 {
			name = "PKCS8"
		}
		t.Run(name, func(t *testing.T) {
			keyFile, record := writeRSAKey(t, pkcs8)
			configureDKIM(t, keyFile)

			signed, err := SignOutboundDKIM([]byte(testMessage))
			require.NoError(t, err)
			assert.True(t, bytes.HasSuffix(signed, []byte(testMessage)), "the original message must be preserved verbatim")
			assert.Contains(t, string(signed), "DKIM-Signature:")

			verifications, err := dkim.VerifyWithOptions(bytes.NewReader(signed), &dkim.VerifyOptions{
				LookupTXT: func(domain string) ([]string, error) {
					assert.Equal(t, "gitea._domainkey.example.com", domain)
					return []string{record}, nil
				},
			})
			require.NoError(t, err)
			require.Len(t, verifications, 1)
			require.NoError(t, verifications[0].Err)
			assert.Equal(t, "example.com", verifications[0].Domain)
		})
	}
}

func TestSignOutboundDKIMDetectsTampering(t *testing.T) {
	keyFile, record := writeRSAKey(t, false)
	configureDKIM(t, keyFile)

	signed, err := SignOutboundDKIM([]byte(testMessage))
	require.NoError(t, err)
	tampered := bytes.Replace(signed, []byte("body line one"), []byte("body line ONE"), 1)
	require.NotEqual(t, signed, tampered)

	verifications, err := dkim.VerifyWithOptions(bytes.NewReader(tampered), &dkim.VerifyOptions{
		LookupTXT: func(string) ([]string, error) { return []string{record}, nil },
	})
	require.NoError(t, err)
	require.Len(t, verifications, 1)
	assert.Error(t, verifications[0].Err, "a modified body must not verify")
}

func TestSignOutboundDKIMDisabled(t *testing.T) {
	t.Cleanup(test.MockVariableValue(&setting.MailboxServer.DKIMEnabled, false))
	signed, err := SignOutboundDKIM([]byte(testMessage))
	require.NoError(t, err)
	assert.Equal(t, testMessage, string(signed), "signing must be a no-op when disabled")
}

func TestParseDKIMPrivateKey(t *testing.T) {
	t.Run("Ed25519", func(t *testing.T) {
		_, priv, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		der, err := x509.MarshalPKCS8PrivateKey(priv)
		require.NoError(t, err)
		signer, err := parseDKIMPrivateKey(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
		require.NoError(t, err)
		assert.IsType(t, ed25519.PublicKey{}, signer.Public())
	})

	t.Run("NotPEM", func(t *testing.T) {
		_, err := parseDKIMPrivateKey([]byte("this is not a key"))
		assert.ErrorContains(t, err, "not PEM encoded")
	})

	t.Run("UnsupportedType", func(t *testing.T) {
		_, err := parseDKIMPrivateKey(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("garbage")}))
		assert.Error(t, err)
	})
}

func TestCanonicalization(t *testing.T) {
	for value, expected := range map[string]dkim.Canonicalization{
		"":        dkim.CanonicalizationRelaxed,
		"relaxed": dkim.CanonicalizationRelaxed,
		"simple":  dkim.CanonicalizationSimple,
	} {
		got, err := canonicalization(value)
		require.NoError(t, err, value)
		assert.Equal(t, expected, got, value)
	}
	_, err := canonicalization("bogus")
	assert.ErrorContains(t, err, "unsupported value")
}

func TestDomainAligned(t *testing.T) {
	cases := []struct {
		auth, from string
		mode       dmarc.AlignmentMode
		aligned    bool
	}{
		{"example.com", "example.com", dmarc.AlignmentStrict, true},
		{"mail.example.com", "example.com", dmarc.AlignmentStrict, false},
		{"mail.example.com", "example.com", dmarc.AlignmentRelaxed, true},
		{"example.com", "mail.example.com", dmarc.AlignmentRelaxed, true},
		{"evil.com", "example.com", dmarc.AlignmentRelaxed, false},
		// A shared public suffix is not an organizational match.
		{"a.co.uk", "b.co.uk", dmarc.AlignmentRelaxed, false},
		{"", "example.com", dmarc.AlignmentRelaxed, false},
		{"example.com", "", dmarc.AlignmentRelaxed, false},
	}
	for _, c := range cases {
		assert.Equal(t, c.aligned, domainAligned(c.auth, c.from, c.mode), "%s vs %s", c.auth, c.from)
	}
}

func TestNormalizeDomain(t *testing.T) {
	assert.Equal(t, "example.com", normalizeDomain("  Example.COM.  "))
	assert.Equal(t, "xn--bcher-kva.example", normalizeDomain("Bücher.example"))
	assert.Empty(t, normalizeDomain(""))
}

func TestRFC5322FromDomain(t *testing.T) {
	domain, err := rfc5322FromDomain([]byte(testMessage))
	require.NoError(t, err)
	assert.Equal(t, "example.com", domain)

	// DMARC is only defined for a single RFC5322.From mailbox.
	_, err = rfc5322FromDomain([]byte("From: a@example.com, b@example.org\r\nSubject: x\r\n\r\nbody\r\n"))
	assert.ErrorContains(t, err, "exactly one mailbox")

	_, err = rfc5322FromDomain([]byte("Subject: no from header\r\n\r\nbody\r\n"))
	assert.Error(t, err)
}

func TestSMTPIdentityDomain(t *testing.T) {
	assert.Equal(t, "example.com", smtpIdentityDomain("bounce@example.com", "helo.example.net"))
	// A null reverse path falls back to the HELO identity.
	assert.Equal(t, "helo.example.net", smtpIdentityDomain("", "helo.example.net"))
	assert.Equal(t, "192.0.2.1", smtpIdentityDomain("", "[192.0.2.1]"))
}

func TestDMARCSampleApplies(t *testing.T) {
	pct := func(v int) *int { return &v }
	assert.True(t, dmarcSampleApplies(&dmarc.Record{}, []byte("m"), nil))
	assert.True(t, dmarcSampleApplies(&dmarc.Record{Percent: pct(100)}, []byte("m"), nil))
	assert.False(t, dmarcSampleApplies(&dmarc.Record{Percent: pct(0)}, []byte("m"), nil))

	// A partial percentage must be deterministic for the same message.
	record := &dmarc.Record{Percent: pct(50)}
	first := dmarcSampleApplies(record, []byte("message"), nil)
	assert.Equal(t, first, dmarcSampleApplies(record, []byte("message"), nil))
}

func TestCleanAuthValue(t *testing.T) {
	// Header injection and Authentication-Results field separators must not survive.
	assert.Equal(t, "evil_ spf=pass", cleanAuthValue("evil;\r\n spf=pass"))
	assert.Len(t, cleanAuthValue(string(make([]byte, 900))), 512)
}

func TestPrependMessageHeader(t *testing.T) {
	assert.Equal(t, "X-A: b\r\nbody", string(prependMessageHeader([]byte("body"), "X-A: b")))
	assert.Equal(t, "X-A: b\r\nbody", string(prependMessageHeader([]byte("body"), "X-A: b\r\n")))
	assert.Equal(t, "body", string(prependMessageHeader([]byte("body"), "")))
}
