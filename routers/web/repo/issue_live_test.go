// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package repo

import (
	"testing"

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
	<div class="timeline-item event">target branch changed</div>
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

func TestIssueLiveDiffSnapshot(t *testing.T) {
	initial, err := issueLiveParseSnapshot(`
<div data-issue-live-snapshot>
	<div class="timeline-item event" id="issuecomment-10">first</div>
	<div class="timeline-item event" id="issuecomment-11">second</div>
</div>`)
	require.NoError(t, err)

	operations, previous := issueLiveDiffSnapshot(nil, initial, true)
	require.Len(t, operations, 2)
	assert.Equal(t, "upsert", operations[0].Action)
	assert.Equal(t, "issuecomment-10", operations[0].Key)
	assert.Equal(t, "issuecomment-11", operations[0].BeforeKey)

	updated, err := issueLiveParseSnapshot(`
<div data-issue-live-snapshot>
	<div class="timeline-item event" id="issuecomment-10">first edited</div>
	<div class="timeline-item event" id="issuecomment-12">third</div>
</div>`)
	require.NoError(t, err)

	operations, _ = issueLiveDiffSnapshot(previous, updated, false)
	require.Len(t, operations, 3)
	assert.Equal(t, issueLiveOperation{
		Action:    "upsert",
		Key:       "issuecomment-10",
		BeforeKey: "issuecomment-12",
		HTML:      updated[0].HTML,
	}, operations[0])
	assert.Equal(t, issueLiveOperation{
		Action: "upsert",
		Key:    "issuecomment-12",
		HTML:   updated[1].HTML,
	}, operations[1])
	assert.Equal(t, issueLiveOperation{Action: "delete", Key: "issuecomment-11"}, operations[2])
}
