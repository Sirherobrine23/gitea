// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package websocket

import (
	"testing"

	issues_model "gitea.dev/models/issues"
	"gitea.dev/services/pubsub"

	"github.com/stretchr/testify/assert"
)

func TestIssueNotifierPublishesTimelineInvalidation(t *testing.T) {
	previousBroker := pubsub.DefaultBroker
	pubsub.DefaultBroker = pubsub.NewMemoryBroker()
	t.Cleanup(func() { pubsub.DefaultBroker = previousBroker })

	refresh, cancel := pubsub.DefaultBroker.Subscribe(pubsub.IssueTopic(10, 20))
	t.Cleanup(cancel)

	// comments created outside of the web UI (e.g. API) must reach the live timeline too
	(&wsNotifier{}).CreateIssueComment(t.Context(), nil, nil, &issues_model.Issue{RepoID: 10, Index: 20}, nil, nil)
	(&wsNotifier{}).IssueChangeTitle(t.Context(), nil, &issues_model.Issue{RepoID: 10, Index: 21}, "")
	assert.Len(t, refresh, 1)
}
