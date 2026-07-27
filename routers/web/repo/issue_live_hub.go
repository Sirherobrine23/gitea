// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package repo

import (
	"fmt"

	issues_model "gitea.dev/models/issues"
	"gitea.dev/services/pubsub"
)

func issueLiveTopic(repoID, index int64) string {
	return fmt.Sprintf("issue-live-%d-%d", repoID, index)
}

func registerIssueLiveRefresh(repoID, index int64) (<-chan []byte, func()) {
	return pubsub.DefaultBroker.Subscribe(issueLiveTopic(repoID, index))
}

func notifyIssueLive(repoID, index int64) {
	pubsub.DefaultBroker.Publish(issueLiveTopic(repoID, index), []byte{1})
}

func notifyIssueLiveIssue(issue *issues_model.Issue) {
	if issue != nil {
		notifyIssueLive(issue.RepoID, issue.Index)
	}
}

func notifyIssueLiveIssues(issues issues_model.IssueList) {
	for _, issue := range issues {
		notifyIssueLiveIssue(issue)
	}
}
