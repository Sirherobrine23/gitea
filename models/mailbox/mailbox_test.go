// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package mailbox

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalizeFolder(t *testing.T) {
	// System folder names are case-insensitive so IMAP clients can send "inbox".
	assert.Equal(t, FolderInbox, NormalizeFolder("inbox"))
	assert.Equal(t, FolderInbox, NormalizeFolder("  InBoX  "))
	assert.Equal(t, FolderJunk, NormalizeFolder("junk"))
	assert.Equal(t, FolderSent, NormalizeFolder("SENT"))
	// User folders keep their own casing.
	assert.Equal(t, "Projects/Gitea", NormalizeFolder("Projects/Gitea"))
	assert.Equal(t, "projects", NormalizeFolder("projects"))
}

func TestIsSystemFolder(t *testing.T) {
	for _, name := range []string{"INBOX", "inbox", "Sent", "Drafts", "Trash", "Archive", "Junk"} {
		assert.True(t, IsSystemFolder(name), name)
	}
	for _, name := range []string{"", "Projects", "INBOX/sub"} {
		assert.False(t, IsSystemFolder(name), name)
	}
}

func TestValidLocalPart(t *testing.T) {
	for _, valid := range []string{"info", "no-reply", "a.b.c", "user+tag", "x"} {
		assert.True(t, validLocalPart(valid), valid)
	}
	for _, invalid := range []string{
		"",               // empty
		"Upper",          // must already be lowercased by the caller
		".leading",       // RFC 5322 dot-atom rules
		"trailing.",      //
		"double..dot",    //
		"has space",      //
		"has@at",         //
		"quote\"d",       //
		"inject\r\nRCPT", // header/command injection
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", // over 64 octets
	} {
		assert.False(t, validLocalPart(invalid), "%q", invalid)
	}
}

func TestHashInternetMessageID(t *testing.T) {
	// The hash backs the de-duplication index, so it must be stable and distinct.
	assert.Equal(t, hashInternetMessageID("<a@example.com>"), hashInternetMessageID("<a@example.com>"))
	assert.NotEqual(t, hashInternetMessageID("<a@example.com>"), hashInternetMessageID("<b@example.com>"))
	assert.Len(t, hashInternetMessageID("<a@example.com>"), 64)
}

func TestNewUIDValidity(t *testing.T) {
	// Zero is not a legal UIDVALIDITY value.
	for range 32 {
		assert.NotZero(t, newUIDValidity())
	}
}

func TestOutboundRecipientRoundTrip(t *testing.T) {
	out := &Outbound{}
	assert.Nil(t, out.RecipientList())

	out.SetRecipients([]string{"a@example.com", "b@example.com"})
	assert.Equal(t, []string{"a@example.com", "b@example.com"}, out.RecipientList())

	out.SetRecipients(nil)
	assert.Nil(t, out.RecipientList())
}

func TestAliasKindsAreDistinct(t *testing.T) {
	// The admin page and the retirement path key off these values.
	assert.NotEqual(t, AliasKindManual, AliasKindRetired)
	assert.Equal(t, "manual", AliasKindManual)
	assert.Equal(t, "retired", AliasKindRetired)
}

func TestValidLocalPartGuardsRetirement(t *testing.T) {
	// RetireLocalPart silently skips names it could never deliver to, so a rename
	// away from an unusual username cannot fail the rename itself.
	assert.False(t, validLocalPart("has space"))
	assert.False(t, validLocalPart("UPPER"))
	assert.True(t, validLocalPart("old-name"))
}
