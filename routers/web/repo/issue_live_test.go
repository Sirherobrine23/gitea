// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package repo

import (
	"testing"
	"time"

	"gitea.dev/services/pubsub"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIssueLiveParseSnapshot(t *testing.T) {
	entries, err := issueLiveParseSnapshot(`
<div data-issue-live-snapshot>
	<div class="timeline-item comment issue-content-comment" id="issue-4">issue</div>
	<div class="timeline-item event" id="issuecomment-10">event</div>
	<div class="timeline-item commits-list"><div id="issuecomment-10-0">commit</div></div>
	<div class="timeline-item-group"><div class="timeline-item" id="issuecomment-11">review</div></div>
	<div class="timeline-item event">unkeyed event</div>
</div>`)
	require.NoError(t, err)
	require.Len(t, entries, 5)

	assert.Equal(t, "issue-4", entries[0].Key)
	assert.Equal(t, "issuecomment-10", entries[1].Key)
	assert.Equal(t, "issuecomment-10:commits", entries[2].Key)
	assert.Equal(t, "issuecomment-11", entries[3].Key)
	assert.Equal(t, "unkeyed:0", entries[4].Key)

	assert.Equal(t, entries[1].Key, entries[0].BeforeKey)
	assert.Equal(t, entries[2].Key, entries[1].BeforeKey)
	assert.Equal(t, entries[3].Key, entries[2].BeforeKey)
	assert.Equal(t, entries[4].Key, entries[3].BeforeKey)
	assert.Empty(t, entries[4].BeforeKey)
}

func TestIssueLiveParseSnapshotRejectsDuplicateKeys(t *testing.T) {
	_, err := issueLiveParseSnapshot(`
<div data-issue-live-snapshot>
	<div class="timeline-item event" id="issuecomment-10">first</div>
	<div class="timeline-item event" id="issuecomment-10">second</div>
</div>`)
	require.ErrorContains(t, err, `duplicate live timeline key "issuecomment-10"`)
}

func TestIssueLiveDiffSnapshot(t *testing.T) {
	initial, err := issueLiveParseSnapshot(`
<div data-issue-live-snapshot>
	<div class="timeline-item event" id="issuecomment-10">first</div>
	<div class="timeline-item event" id="issuecomment-11">second</div>
</div>`)
	require.NoError(t, err)
	previous := issueLiveSnapshotStates(initial)

	updated, err := issueLiveParseSnapshot(`
<div data-issue-live-snapshot>
	<div class="timeline-item event" id="issuecomment-10">first edited</div>
	<div class="timeline-item event" id="issuecomment-12">third</div>
</div>`)
	require.NoError(t, err)

	operations, _ := issueLiveDiffSnapshot(previous, updated)
	require.Len(t, operations, 3)
	assert.Equal(t, issueLiveOperation{
		Action:    "upsert",
		Key:       "issuecomment-10",
		BeforeKey: "issuecomment-12",
		HTML:      updated[0].HTML,
		Hash:      issueLiveHashString(updated[0].Hash),
	}, operations[0])
	assert.Equal(t, issueLiveOperation{
		Action: "upsert",
		Key:    "issuecomment-12",
		HTML:   updated[1].HTML,
		Hash:   issueLiveHashString(updated[1].Hash),
	}, operations[1])
	assert.Equal(t, issueLiveOperation{Action: "delete", Key: "issuecomment-11"}, operations[2])
}

func TestIssueLiveDiffSnapshotDetectsReorder(t *testing.T) {
	initial, err := issueLiveParseSnapshot(`
<div data-issue-live-snapshot>
	<div class="timeline-item event" id="issuecomment-10">first</div>
	<div class="timeline-item event" id="issuecomment-11">second</div>
</div>`)
	require.NoError(t, err)

	reordered, err := issueLiveParseSnapshot(`
<div data-issue-live-snapshot>
	<div class="timeline-item event" id="issuecomment-11">second</div>
	<div class="timeline-item event" id="issuecomment-10">first</div>
</div>`)
	require.NoError(t, err)

	operations, _ := issueLiveDiffSnapshot(issueLiveSnapshotStates(initial), reordered)
	require.Len(t, operations, 2)
	assert.Equal(t, "issuecomment-11", operations[0].Key)
	assert.Equal(t, "issuecomment-10", operations[0].BeforeKey)
	assert.Equal(t, "issuecomment-10", operations[1].Key)
	assert.Empty(t, operations[1].BeforeKey)
}

func TestIssueLiveDiffSnapshotKeepsIssueDescription(t *testing.T) {
	initial, err := issueLiveParseSnapshot(`
<div data-issue-live-snapshot>
	<div class="timeline-item comment issue-content-comment" id="issue-4">
		<div class="comment-body">old description</div>
	</div>
</div>`)
	require.NoError(t, err)

	updated, err := issueLiveParseSnapshot(`
<div data-issue-live-snapshot>
	<div class="timeline-item comment issue-content-comment" id="issue-4">
		<div class="comment-body">new description</div>
	</div>
</div>`)
	require.NoError(t, err)

	operations, current := issueLiveDiffSnapshot(issueLiveSnapshotStates(initial), updated)
	require.Len(t, operations, 1)
	assert.Equal(t, "upsert", operations[0].Action)
	assert.Equal(t, "issue-4", operations[0].Key)
	assert.Contains(t, operations[0].HTML, "new description")
	assert.Contains(t, current, "issue-4")
}

func TestIssueLiveRefreshUsesPubsubBroker(t *testing.T) {
	previousBroker := pubsub.DefaultBroker
	pubsub.DefaultBroker = pubsub.NewMemoryBroker()
	t.Cleanup(func() { pubsub.DefaultBroker = previousBroker })

	refresh, cancel := registerIssueLiveRefresh(10, 20)
	t.Cleanup(cancel)

	notifyIssueLive(10, 20)
	select {
	case <-refresh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for issue live pub/sub invalidation")
	}

	notifyIssueLive(10, 21)
	select {
	case <-refresh:
		t.Fatal("received invalidation for a different issue topic")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestIssueLiveClientStateRoundTrip(t *testing.T) {
	entries, err := issueLiveParseSnapshot(`
<div data-issue-live-snapshot>
	<div class="timeline-item event" id="issuecomment-10">first</div>
	<div class="timeline-item event" id="issuecomment-11">second</div>
</div>`)
	require.NoError(t, err)

	serverStates := issueLiveServerStates(entries, nil)
	clientStates := make([]issueLiveClientState, 0, len(serverStates))
	for _, state := range serverStates {
		clientStates = append(clientStates, issueLiveClientState{
			Key:             state.Key,
			BeforeKey:       state.BeforeKey,
			Hash:            state.Hash,
			ContentVersion:  state.ContentVersion,
			ReactionState:   state.ReactionState,
			AttachmentState: state.AttachmentState,
		})
	}

	decoded, err := issueLiveDecodeClientStates(clientStates)
	require.NoError(t, err)
	assert.Equal(t, issueLiveSnapshotStates(entries), decoded)
}

func TestIssueLiveDecodeClientStatesRejectsInvalidHash(t *testing.T) {
	_, err := issueLiveDecodeClientStates([]issueLiveClientState{{Key: "issuecomment-10", Hash: "invalid"}})
	require.ErrorContains(t, err, `invalid issue live hash for "issuecomment-10"`)
}

func TestIssueLiveInitialSnapshotStates(t *testing.T) {
	entries, err := issueLiveParseSnapshot(`
<div data-issue-live-snapshot>
	<div class="timeline-item comment" id="issuecomment-10">
		<div class="edit-content-zone" data-content-version="3"></div>
		<div class="bottom-reactions">
			<a data-reaction-content="+1" data-has-reacted="true"><span class="reaction-count">2</span></a>
		</div>
		<div class="dropzone-attachments"><a href="/attachments/a">a</a></div>
	</div>
</div>`)
	require.NoError(t, err)
	require.Len(t, entries, 1)

	matching := []issueLiveClientState{{
		Key:             entries[0].Key,
		ContentVersion:  entries[0].ContentVersion,
		ReactionState:   entries[0].ReactionState,
		AttachmentState: entries[0].AttachmentState,
	}}
	assert.Equal(t, entries[0].Hash, issueLiveInitialSnapshotStates(matching, entries)[entries[0].Key].Hash)

	changed := append([]issueLiveClientState(nil), matching...)
	changed[0].ContentVersion = "2"
	assert.Zero(t, issueLiveInitialSnapshotStates(changed, entries)[entries[0].Key].Hash)
}

func TestIssueLiveServerStatesCanExcludePendingUpserts(t *testing.T) {
	entries, err := issueLiveParseSnapshot(`
<div data-issue-live-snapshot>
	<div class="timeline-item event" id="issuecomment-10">first</div>
	<div class="timeline-item event" id="issuecomment-11">second</div>
</div>`)
	require.NoError(t, err)

	states := issueLiveServerStates(entries, map[string]struct{}{"issuecomment-11": {}})
	require.Len(t, states, 1)
	assert.Equal(t, "issuecomment-10", states[0].Key)
}
