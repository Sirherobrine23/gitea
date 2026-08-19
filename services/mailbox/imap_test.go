// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package mailbox

import (
	"testing"

	mailbox_model "gitea.dev/models/mailbox"

	"github.com/emersion/go-imap/v2"
	"github.com/stretchr/testify/assert"
)

func TestMessageFlags(t *testing.T) {
	assert.Empty(t, messageFlags(&mailbox_model.Message{}))

	flags := messageFlags(&mailbox_model.Message{Seen: true, Answered: true, Flagged: true, Deleted: true, Draft: true})
	for _, want := range supportedIMAPFlags() {
		assert.True(t, hasFlag(flags, want), "%v missing", want)
	}
}

func TestApplyStore(t *testing.T) {
	current := []imap.Flag{imap.FlagSeen}

	added := applyStore(current, &imap.StoreFlags{Op: imap.StoreFlagsAdd, Flags: []imap.Flag{imap.FlagFlagged}})
	assert.True(t, hasFlag(added, imap.FlagSeen))
	assert.True(t, hasFlag(added, imap.FlagFlagged))

	removed := applyStore(added, &imap.StoreFlags{Op: imap.StoreFlagsDel, Flags: []imap.Flag{imap.FlagSeen}})
	assert.False(t, hasFlag(removed, imap.FlagSeen))
	assert.True(t, hasFlag(removed, imap.FlagFlagged))

	// SET replaces the whole set rather than merging into it.
	set := applyStore(added, &imap.StoreFlags{Op: imap.StoreFlagsSet, Flags: []imap.Flag{imap.FlagDraft}})
	assert.Equal(t, []imap.Flag{imap.FlagDraft}, set)

	// Flags the mailbox does not support are not invented into the result.
	unknown := applyStore(nil, &imap.StoreFlags{Op: imap.StoreFlagsAdd, Flags: []imap.Flag{imap.Flag("\\Custom")}})
	assert.Empty(t, unknown)
}

func TestSpecialUseAttr(t *testing.T) {
	// RFC 6154 attributes are how a client knows which folder is Sent or Trash.
	assert.Equal(t, imap.MailboxAttrSent, specialUseAttr("Sent"))
	assert.Equal(t, imap.MailboxAttrTrash, specialUseAttr("trash"))
	assert.Equal(t, imap.MailboxAttrJunk, specialUseAttr(mailbox_model.FolderJunk))
	assert.Equal(t, imap.MailboxAttrDrafts, specialUseAttr("Drafts"))
	assert.Equal(t, imap.MailboxAttrArchive, specialUseAttr("Archive"))
	// INBOX has no special-use attribute, and neither do user folders.
	assert.Empty(t, specialUseAttr("INBOX"))
	assert.Empty(t, specialUseAttr("Projects/Gitea"))
}

func TestStaticNumRange(t *testing.T) {
	// "*" is encoded as 0 and must resolve to the last message.
	start, stop := uint32(2), uint32(0)
	staticNumRange(&start, &stop, 9)
	assert.Equal(t, uint32(2), start)
	assert.Equal(t, uint32(9), stop)

	// A range written "*:2" arrives reversed and must be normalized.
	start, stop = 0, 2
	staticNumRange(&start, &stop, 9)
	assert.Equal(t, uint32(2), start)
	assert.Equal(t, uint32(9), stop)

	// A fully static range is left alone.
	start, stop = 3, 5
	staticNumRange(&start, &stop, 9)
	assert.Equal(t, uint32(3), start)
	assert.Equal(t, uint32(5), stop)
}

func TestTrackerRefCounting(t *testing.T) {
	key := mailboxKey{userID: 4242, folder: "INBOX"}
	t.Cleanup(func() {
		trackers.Lock()
		delete(trackers.m, key)
		trackers.Unlock()
	})

	first := acquireTracker(key, 0)
	second := acquireTracker(key, 0)
	assert.Same(t, first, second, "sessions on one folder must share a tracker")
	assert.Same(t, first, lookupTracker(key))

	// The tracker only goes away once the last session releases it.
	releaseTracker(key)
	assert.Same(t, first, lookupTracker(key))
	releaseTracker(key)
	assert.Nil(t, lookupTracker(key), "an unwatched folder keeps no tracker")
}
