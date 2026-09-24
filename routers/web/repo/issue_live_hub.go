// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package repo

import (
	issues_model "gitea.dev/models/issues"
	"gitea.dev/services/pubsub"
)

func registerIssueLiveRefresh(repoID, index int64) (<-chan []byte, func()) {
	return pubsub.DefaultBroker.Subscribe(pubsub.IssueTopic(repoID, index))
}

func notifyIssueLive(repoID, index int64) {
	pubsub.DefaultBroker.Publish(pubsub.IssueTopic(repoID, index), []byte{1})
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
