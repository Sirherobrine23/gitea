// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package repo

import (
	stdcontext "context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	issues_model "gitea.dev/models/issues"
	"gitea.dev/models/unit"
	user_model "gitea.dev/models/user"
	"gitea.dev/modules/log"
	"gitea.dev/modules/setting"
	"gitea.dev/modules/templates"
	"gitea.dev/services/context"
	user_service "gitea.dev/services/user"

	"github.com/coder/websocket"
	nethtml "golang.org/x/net/html"
)

const tplIssueLiveComments templates.TplName = "repo/issue/view_content/comments_live"

const (
	issueLiveRefreshInterval = 2 * time.Second
	issueLivePingInterval    = 25 * time.Second
	issueLiveWriteTimeout    = 10 * time.Second
)

type issueLiveClientMessage struct {
	Type string `json:"type"`
}

type issueLiveOperation struct {
	Action    string `json:"action"`
	Key       string `json:"key"`
	BeforeKey string `json:"beforeKey,omitempty"`
	HTML      string `json:"html,omitempty"`
}

type issueLiveServerMessage struct {
	Type       string               `json:"type"`
	Sequence   uint64               `json:"sequence"`
	Operations []issueLiveOperation `json:"operations"`
}

type issueLiveSnapshotEntry struct {
	Key       string
	BeforeKey string
	HTML      string
	Hash      [sha256.Size]byte
}

func isIssueLiveWebSocketRequest(req *http.Request) bool {
	return strings.EqualFold(req.Header.Get("Upgrade"), "websocket") &&
		strings.Contains(strings.ToLower(req.Header.Get("Connection")), "upgrade")
}

func prepareIssueLiveSnapshot(ctx *context.Context) (string, error) {
	issue, err := issues_model.GetIssueByIndex(ctx, ctx.Repo.Repository.ID, ctx.PathParamInt64("index"))
	if err != nil {
		return "", err
	}
	issue.Repo = ctx.Repo.Repository

	if err := issue.LoadPullRequest(ctx); err != nil {
		return "", err
	}
	if issue.IsPull {
		if !ctx.Repo.Permission.CanRead(unit.TypePullRequests) {
			return "", errors.New("permission denied while reading pull request")
		}
		if ctx.PathParam("type") != "pulls" {
			return "", errors.New("issue type does not match pull request")
		}
		if issue.PullRequest == nil {
			return "", errors.New("pull request data was not loaded")
		}
		if err := issue.PullRequest.LoadBaseRepo(ctx); err != nil {
			return "", err
		}
	} else {
		if !ctx.Repo.Permission.CanRead(unit.TypeIssues) {
			return "", errors.New("permission denied while reading issue")
		}
		if ctx.PathParam("type") != "issues" {
			return "", errors.New("issue type does not match issue")
		}
	}

	if err := issue.LoadAttributes(ctx); err != nil {
		return "", err
	}
	if err := filterXRefComments(ctx, issue); err != nil {
		return "", err
	}

	ctx.Data["Issue"] = issue
	ctx.Data["IsAttachmentEnabled"] = setting.Attachment.Enabled
	ctx.Data["IsIssuePoster"] = ctx.IsSigned && issue.IsPoster(ctx.Doer.ID)
	ctx.Data["HasIssuesOrPullsWritePermission"] = ctx.Repo.Permission.CanWriteIssuesOrPulls(issue.IsPull)
	ctx.Data["HasProjectsWritePermission"] = ctx.Repo.Permission.CanWrite(unit.TypeProjects)
	ctx.Data["IsRepoAdmin"] = ctx.IsSigned && (ctx.Repo.Permission.IsAdmin() || ctx.Doer.IsAdmin)
	ctx.Data["CanBlockUser"] = func(blocker, blockee *user_model.User) bool {
		return user_service.CanBlockUser(ctx, ctx.Doer, blocker, blockee)
	}
	if issue.IsPull {
		ctx.Data["BaseTarget"] = issue.PullRequest.BaseBranch
	}

	prepareIssueViewCommentsAndSidebarParticipants(ctx, issue)

	rendered, err := ctx.RenderToHTML(tplIssueLiveComments, ctx.Data)
	if err != nil {
		return "", err
	}
	return string(rendered), nil
}

func issueLiveNodeAttribute(node *nethtml.Node, name string) string {
	for _, attribute := range node.Attr {
		if attribute.Key == name {
			return attribute.Val
		}
	}
	return ""
}

func issueLiveNodeHasClass(node *nethtml.Node, className string) bool {
	for _, current := range strings.Fields(issueLiveNodeAttribute(node, "class")) {
		if current == className {
			return true
		}
	}
	return false
}

func issueLiveIsCommentKey(value string) bool {
	for _, prefix := range []string{"issue-", "pull-", "issuecomment-", "pullcomment-"} {
		if !strings.HasPrefix(value, prefix) {
			continue
		}
		_, err := strconv.ParseInt(strings.TrimPrefix(value, prefix), 10, 64)
		return err == nil
	}
	return false
}

func issueLiveFindCommentKey(node *nethtml.Node) string {
	if key := issueLiveNodeAttribute(node, "id"); issueLiveIsCommentKey(key) {
		return key
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		if key := issueLiveFindCommentKey(child); key != "" {
			return key
		}
	}
	return ""
}

func issueLiveFindSnapshotNode(node *nethtml.Node) *nethtml.Node {
	if issueLiveNodeAttribute(node, "data-issue-live-snapshot") != "" ||
		(node.Type == nethtml.ElementNode && issueLiveNodeAttribute(node, "data-issue-live-snapshot") == "") {
		for _, attribute := range node.Attr {
			if attribute.Key == "data-issue-live-snapshot" {
				return node
			}
		}
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		if found := issueLiveFindSnapshotNode(child); found != nil {
			return found
		}
	}
	return nil
}

func issueLiveRenderNode(node *nethtml.Node) (string, error) {
	var rendered strings.Builder
	if err := nethtml.Render(&rendered, node); err != nil {
		return "", err
	}
	return rendered.String(), nil
}

func issueLiveParseSnapshot(rendered string) ([]issueLiveSnapshotEntry, error) {
	document, err := nethtml.Parse(strings.NewReader(rendered))
	if err != nil {
		return nil, err
	}
	snapshot := issueLiveFindSnapshotNode(document)
	if snapshot == nil {
		return nil, errors.New("live timeline snapshot root was not rendered")
	}

	entries := make([]issueLiveSnapshotEntry, 0, 16)
	previousCommentKey := ""
	unkeyedIndex := 0
	for node := snapshot.FirstChild; node != nil; node = node.NextSibling {
		if node.Type != nethtml.ElementNode {
			continue
		}

		key := issueLiveFindCommentKey(node)
		if key != "" {
			previousCommentKey = key
		} else if previousCommentKey != "" && issueLiveNodeHasClass(node, "timeline-item") && issueLiveNodeHasClass(node, "commits-list") {
			key = previousCommentKey + ":commits"
		} else if issueLiveNodeHasClass(node, "timeline-item") || issueLiveNodeHasClass(node, "timeline-item-group") {
			key = fmt.Sprintf("unkeyed:%d", unkeyedIndex)
			unkeyedIndex++
		}
		if key == "" {
			continue
		}

		html, err := issueLiveRenderNode(node)
		if err != nil {
			return nil, err
		}
		entries = append(entries, issueLiveSnapshotEntry{
			Key:  key,
			HTML: html,
			Hash: sha256.Sum256([]byte(html)),
		})
	}

	for index := range entries {
		if index+1 < len(entries) {
			entries[index].BeforeKey = entries[index+1].Key
		}
	}
	return entries, nil
}

func issueLiveDiffSnapshot(previous map[string][sha256.Size]byte, entries []issueLiveSnapshotEntry, force bool) ([]issueLiveOperation, map[string][sha256.Size]byte) {
	current := make(map[string][sha256.Size]byte, len(entries))
	operations := make([]issueLiveOperation, 0, len(entries))

	for _, entry := range entries {
		current[entry.Key] = entry.Hash
		oldHash, exists := previous[entry.Key]
		if force || !exists || oldHash != entry.Hash {
			operations = append(operations, issueLiveOperation{
				Action:    "upsert",
				Key:       entry.Key,
				BeforeKey: entry.BeforeKey,
				HTML:      entry.HTML,
			})
		}
	}
	for key := range previous {
		if _, exists := current[key]; !exists {
			operations = append(operations, issueLiveOperation{Action: "delete", Key: key})
		}
	}
	return operations, current
}

func issueLiveWrite(ctx stdcontext.Context, conn *websocket.Conn, message issueLiveServerMessage) error {
	payload, err := json.Marshal(message)
	if err != nil {
		return err
	}

	writeCtx, cancel := stdcontext.WithTimeout(ctx, issueLiveWriteTimeout)
	defer cancel()
	return conn.Write(writeCtx, websocket.MessageText, payload)
}

func issueLiveReadLoop(ctx stdcontext.Context, cancel stdcontext.CancelFunc, conn *websocket.Conn, refresh chan<- struct{}) {
	defer cancel()
	for {
		_, payload, err := conn.Read(ctx)
		if err != nil {
			return
		}

		var message issueLiveClientMessage
		if json.Unmarshal(payload, &message) != nil || message.Type != "refresh" {
			continue
		}
		select {
		case refresh <- struct{}{}:
		default:
		}
	}
}

// IssueLive serves incremental issue and pull-request timeline operations over
// a WebSocket connection. The server renders the existing timeline templates,
// splits them into stable activity fragments and only sends changed fragments.
func IssueLive(ctx *context.Context) {
	initialHTML, err := prepareIssueLiveSnapshot(ctx)
	if err != nil {
		ctx.ServerError("prepareIssueLiveSnapshot", err)
		return
	}
	if ctx.Written() {
		return
	}

	conn, err := websocket.Accept(ctx.Resp, ctx.Req, nil)
	if err != nil {
		log.Warn("Unable to accept issue live WebSocket: %v", err)
		return
	}
	defer conn.CloseNow()
	conn.SetReadLimit(1024)

	connectionCtx, cancel := stdcontext.WithCancel(stdcontext.Background())
	defer cancel()

	refresh := make(chan struct{}, 1)
	unregisterRefresh := registerIssueLiveRefresh(ctx.Repo.Repository.ID, ctx.PathParamInt64("index"), refresh)
	defer unregisterRefresh()
	go issueLiveReadLoop(connectionCtx, cancel, conn, refresh)

	refreshTicker := time.NewTicker(issueLiveRefreshInterval)
	defer refreshTicker.Stop()
	pingTicker := time.NewTicker(issueLivePingInterval)
	defer pingTicker.Stop()

	var (
		sequence uint64
		previous = make(map[string][sha256.Size]byte)
	)

	sendSnapshot := func(rendered string, force bool) error {
		entries, err := issueLiveParseSnapshot(rendered)
		if err != nil {
			return err
		}
		operations, current := issueLiveDiffSnapshot(previous, entries, force)
		if len(operations) == 0 {
			previous = current
			return nil
		}

		sequence++
		if err := issueLiveWrite(connectionCtx, conn, issueLiveServerMessage{
			Type:       "timeline",
			Sequence:   sequence,
			Operations: operations,
		}); err != nil {
			return err
		}
		previous = current
		return nil
	}

	if err := sendSnapshot(initialHTML, true); err != nil {
		log.Debug("Unable to write initial issue live snapshot: %v", err)
		return
	}

	for {
		select {
		case <-connectionCtx.Done():
			return
		case <-refreshTicker.C:
		case <-refresh:
		case <-pingTicker.C:
			pingCtx, pingCancel := stdcontext.WithTimeout(connectionCtx, issueLiveWriteTimeout)
			err := conn.Ping(pingCtx)
			pingCancel()
			if err != nil {
				return
			}
			continue
		}

		rendered, err := prepareIssueLiveSnapshot(ctx)
		if err != nil {
			log.Warn("Unable to refresh live timeline for %s: %v", ctx.Req.URL.Path, err)
			continue
		}
		if err := sendSnapshot(rendered, false); err != nil {
			if websocket.CloseStatus(err) == -1 {
				log.Debug("Unable to write issue live snapshot: %v", err)
			}
			return
		}
	}
}
