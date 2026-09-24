// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package websocket

import (
	"context"

	issues_model "gitea.dev/models/issues"
	repo_model "gitea.dev/models/repo"
	user_model "gitea.dev/models/user"
	"gitea.dev/modules/log"
	"gitea.dev/services/pubsub"
)

// PublishIssueChanged invalidates the live timeline of an issue. Subscribers render
// the timeline with their own permissions, so the message carries no data.
func PublishIssueChanged(issue *issues_model.Issue) {
	if issue != nil {
		pubsub.DefaultBroker.Publish(pubsub.IssueTopic(issue.RepoID, issue.Index), []byte{1})
	}
}

func publishCommentIssueChanged(ctx context.Context, c *issues_model.Comment) {
	if err := c.LoadIssue(ctx); err != nil {
		log.Error("websocket: LoadIssue of comment %d: %v", c.ID, err)
		return
	}
	PublishIssueChanged(c.Issue)
}

func publishPullRequestChanged(ctx context.Context, pr *issues_model.PullRequest) {
	if err := pr.LoadIssue(ctx); err != nil {
		log.Error("websocket: LoadIssue of pull request %d: %v", pr.ID, err)
		return
	}
	PublishIssueChanged(pr.Issue)
}

func (n *wsNotifier) CreateIssueComment(_ context.Context, _ *user_model.User, _ *repo_model.Repository, issue *issues_model.Issue, _ *issues_model.Comment, _ []*user_model.User) {
	PublishIssueChanged(issue)
}

func (n *wsNotifier) UpdateComment(ctx context.Context, _ *user_model.User, c *issues_model.Comment, _ string) {
	publishCommentIssueChanged(ctx, c)
}

func (n *wsNotifier) DeleteComment(ctx context.Context, _ *user_model.User, c *issues_model.Comment) {
	publishCommentIssueChanged(ctx, c)
}

func (n *wsNotifier) IssueChangeStatus(_ context.Context, _ *user_model.User, _ string, issue *issues_model.Issue, _ *issues_model.Comment, _ bool) {
	PublishIssueChanged(issue)
}

func (n *wsNotifier) IssueChangeMilestone(_ context.Context, _ *user_model.User, issue *issues_model.Issue, _ int64) {
	PublishIssueChanged(issue)
}

func (n *wsNotifier) IssueChangeAssignee(_ context.Context, _ *user_model.User, issue *issues_model.Issue, _ *user_model.User, _ bool, _ *issues_model.Comment) {
	PublishIssueChanged(issue)
}

func (n *wsNotifier) PullRequestReviewRequest(_ context.Context, _ *user_model.User, issue *issues_model.Issue, _ *user_model.User, _ bool, _ *issues_model.Comment) {
	PublishIssueChanged(issue)
}

func (n *wsNotifier) IssueChangeContent(_ context.Context, _ *user_model.User, issue *issues_model.Issue, _ string) {
	PublishIssueChanged(issue)
}

func (n *wsNotifier) IssueClearLabels(_ context.Context, _ *user_model.User, issue *issues_model.Issue) {
	PublishIssueChanged(issue)
}

func (n *wsNotifier) IssueChangeTitle(_ context.Context, _ *user_model.User, issue *issues_model.Issue, _ string) {
	PublishIssueChanged(issue)
}

func (n *wsNotifier) IssueChangeRef(_ context.Context, _ *user_model.User, issue *issues_model.Issue, _ string) {
	PublishIssueChanged(issue)
}

func (n *wsNotifier) IssueChangeLabels(_ context.Context, _ *user_model.User, issue *issues_model.Issue, _, _ []*issues_model.Label) {
	PublishIssueChanged(issue)
}

func (n *wsNotifier) MergePullRequest(ctx context.Context, _ *user_model.User, pr *issues_model.PullRequest) {
	publishPullRequestChanged(ctx, pr)
}

func (n *wsNotifier) AutoMergePullRequest(ctx context.Context, _ *user_model.User, pr *issues_model.PullRequest) {
	publishPullRequestChanged(ctx, pr)
}

func (n *wsNotifier) PullRequestSynchronized(ctx context.Context, _ *user_model.User, pr *issues_model.PullRequest, _, _ string) {
	publishPullRequestChanged(ctx, pr)
}

func (n *wsNotifier) PullRequestReview(ctx context.Context, pr *issues_model.PullRequest, _ *issues_model.Review, _ *issues_model.Comment, _ []*user_model.User) {
	publishPullRequestChanged(ctx, pr)
}

func (n *wsNotifier) PullRequestCodeComment(ctx context.Context, pr *issues_model.PullRequest, _ *issues_model.Comment, _ []*user_model.User) {
	publishPullRequestChanged(ctx, pr)
}

func (n *wsNotifier) PullRequestChangeTargetBranch(ctx context.Context, _ *user_model.User, pr *issues_model.PullRequest, _ string) {
	publishPullRequestChanged(ctx, pr)
}

func (n *wsNotifier) PullRequestPushCommits(ctx context.Context, _ *user_model.User, pr *issues_model.PullRequest, _ *issues_model.Comment) {
	publishPullRequestChanged(ctx, pr)
}

func (n *wsNotifier) PullReviewDismiss(ctx context.Context, _ *user_model.User, review *issues_model.Review, _ *issues_model.Comment) {
	if err := review.LoadIssue(ctx); err != nil {
		log.Error("websocket: LoadIssue of review %d: %v", review.ID, err)
		return
	}
	PublishIssueChanged(review.Issue)
}
