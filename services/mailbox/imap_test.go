// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package mailbox

import (
	"testing"
	"time"

	mailbox_model "gitea.dev/models/mailbox"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/backend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPublishIMAPUpdateNeverBlocks(t *testing.T) {
	original := imapUpdates
	t.Cleanup(func() { imapUpdates = original })

	// With no IMAP listener running there is nothing to notify.
	imapUpdates = nil
	assert.False(t, publishIMAPUpdate(backend.NewUpdate("alice", "INBOX")))

	imapUpdates = make(chan backend.Update, 1)
	assert.True(t, publishIMAPUpdate(backend.NewUpdate("alice", "INBOX")))

	// A consumer that has stopped reading must not stall mail delivery: the
	// second update is dropped rather than blocking the caller.
	done := make(chan bool, 1)
	go func() { done <- publishIMAPUpdate(backend.NewUpdate("alice", "INBOX")) }()
	select {
	case queued := <-done:
		assert.False(t, queued, "a full buffer must drop, not block")
	case <-time.After(2 * time.Second):
		t.Fatal("publishIMAPUpdate blocked on a full buffer")
	}
}

func TestFillMailboxStatusCounts(t *testing.T) {
	folder := &mailbox_model.Folder{UIDValidity: 7, UIDNext: 11}
	msgs := []*mailbox_model.Message{
		{Seen: true, Recent: false},
		{Seen: false, Recent: true},
		{Seen: false, Recent: true},
	}
	status := imap.NewMailboxStatus("INBOX", []imap.StatusItem{
		imap.StatusMessages, imap.StatusRecent, imap.StatusUnseen,
		imap.StatusUidNext, imap.StatusUidValidity,
	})
	fillMailboxStatus(status, folder, msgs)

	assert.Equal(t, uint32(3), status.Messages)
	assert.Equal(t, uint32(2), status.Recent)
	assert.Equal(t, uint32(2), status.Unseen)
	// UNSEEN reports the sequence number of the first unseen message, 1-based.
	assert.Equal(t, uint32(2), status.UnseenSeqNum)
	assert.Equal(t, uint32(7), status.UidValidity)
	assert.Equal(t, uint32(11), status.UidNext)
	require.NotEmpty(t, status.Flags)
}
