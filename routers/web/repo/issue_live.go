// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package repo

import (
	stdcontext "context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	issues_model "gitea.dev/models/issues"
	"gitea.dev/models/unit"
	user_model "gitea.dev/models/user"
	"gitea.dev/modules/graceful"
	"gitea.dev/modules/log"
	"gitea.dev/modules/reqctx"
	"gitea.dev/modules/setting"
	"gitea.dev/modules/templates"
	"gitea.dev/services/context"
	user_service "gitea.dev/services/user"

	"github.com/coder/websocket"
	nethtml "golang.org/x/net/html"
)

const tplIssueLiveComments templates.TplName = "repo/issue/view_content/comments_live"

const (
	issueLiveSafetyRefreshInterval = 60 * time.Second
	issueLivePingInterval          = 25 * time.Second
	issueLiveWriteTimeout          = 10 * time.Second
	issueLiveResumeTimeout         = 10 * time.Second
	issueLiveReadLimit             = 1 << 22
)

type issueLiveClientState struct {
	Key             string `json:"key"`
	BeforeKey       string `json:"beforeKey,omitempty"`
	Hash            string `json:"hash,omitempty"`
	ContentVersion  string `json:"contentVersion,omitempty"`
	ReactionState   string `json:"reactionState,omitempty"`
	AttachmentState string `json:"attachmentState,omitempty"`
}

type issueLiveClientMessage struct {
	Type        string                 `json:"type"`
	Initialized bool                   `json:"initialized,omitempty"`
	States      []issueLiveClientState `json:"states,omitempty"`
}

type issueLiveOperation struct {
	Action    string `json:"action"`
	Key       string `json:"key"`
	BeforeKey string `json:"beforeKey,omitempty"`
	HTML      string `json:"html,omitempty"`
	Hash      string `json:"hash,omitempty"`
}

type issueLiveServerState struct {
	Key             string `json:"key"`
	BeforeKey       string `json:"beforeKey,omitempty"`
	Hash            string `json:"hash"`
	ContentVersion  string `json:"contentVersion,omitempty"`
	ReactionState   string `json:"reactionState,omitempty"`
	AttachmentState string `json:"attachmentState,omitempty"`
}

type issueLiveServerMessage struct {
	Type       string                 `json:"type"`
	Sequence   uint64                 `json:"sequence,omitempty"`
	Operations []issueLiveOperation   `json:"operations,omitempty"`
	States     []issueLiveServerState `json:"states,omitempty"`
}

type issueLiveSnapshotEntry struct {
	Key             string
	BeforeKey       string
	HTML            string
	Hash            [sha256.Size]byte
	ContentVersion  string
	ReactionState   string
	AttachmentState string
}

type issueLiveSnapshotState struct {
	BeforeKey string
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

func issueLiveFindDescendantAttribute(node *nethtml.Node, name string) string {
	if value := issueLiveNodeAttribute(node, name); value != "" {
		return value
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		if value := issueLiveFindDescendantAttribute(child, name); value != "" {
			return value
		}
	}
	return ""
}

func issueLiveNodeText(node *nethtml.Node) string {
	if node.Type == nethtml.TextNode {
		return node.Data
	}
	var text strings.Builder
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		text.WriteString(issueLiveNodeText(child))
	}
	return text.String()
}

func issueLiveFindDescendantByClass(node *nethtml.Node, className string) *nethtml.Node {
	if node.Type == nethtml.ElementNode && issueLiveNodeHasClass(node, className) {
		return node
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		if found := issueLiveFindDescendantByClass(child, className); found != nil {
			return found
		}
	}
	return nil
}

func issueLiveReactionState(node *nethtml.Node) string {
	container := issueLiveFindDescendantByClass(node, "bottom-reactions")
	if container == nil {
		return ""
	}

	parts := make([]string, 0, 4)
	var collect func(*nethtml.Node)
	collect = func(current *nethtml.Node) {
		if current.Type == nethtml.ElementNode {
			if reaction := issueLiveNodeAttribute(current, "data-reaction-content"); reaction != "" {
				count := ""
				if countNode := issueLiveFindDescendantByClass(current, "reaction-count"); countNode != nil {
					count = strings.TrimSpace(issueLiveNodeText(countNode))
				}
				parts = append(parts, reaction+":"+issueLiveNodeAttribute(current, "data-has-reacted")+":"+count)
				return
			}
		}
		for child := current.FirstChild; child != nil; child = child.NextSibling {
			collect(child)
		}
	}
	collect(container)
	sort.Strings(parts)
	return strings.Join(parts, "|")
}

func issueLiveAttachmentState(node *nethtml.Node) string {
	container := issueLiveFindDescendantByClass(node, "dropzone-attachments")
	if container == nil {
		return ""
	}

	links := make(map[string]struct{})
	var collect func(*nethtml.Node)
	collect = func(current *nethtml.Node) {
		if current.Type == nethtml.ElementNode && current.Data == "a" {
			if href := issueLiveNodeAttribute(current, "href"); href != "" {
				links[href] = struct{}{}
			}
		}
		for child := current.FirstChild; child != nil; child = child.NextSibling {
			collect(child)
		}
	}
	collect(container)

	state := make([]string, 0, len(links))
	for href := range links {
		state = append(state, href)
	}
	sort.Strings(state)
	return strings.Join(state, "|")
}

func issueLiveFindSnapshotNode(node *nethtml.Node) *nethtml.Node {
	for _, attribute := range node.Attr {
		if attribute.Key == "data-issue-live-snapshot" {
			return node
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
	seen := make(map[string]struct{})
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
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("duplicate live timeline key %q", key)
		}
		seen[key] = struct{}{}

		html, err := issueLiveRenderNode(node)
		if err != nil {
			return nil, err
		}
		entries = append(entries, issueLiveSnapshotEntry{
			Key:             key,
			HTML:            html,
			Hash:            sha256.Sum256([]byte(html)),
			ContentVersion:  issueLiveFindDescendantAttribute(node, "data-content-version"),
			ReactionState:   issueLiveReactionState(node),
			AttachmentState: issueLiveAttachmentState(node),
		})
	}

	for index := range entries {
		if index+1 < len(entries) {
			entries[index].BeforeKey = entries[index+1].Key
		}
	}
	return entries, nil
}

func issueLiveHashString(hash [sha256.Size]byte) string {
	return hex.EncodeToString(hash[:])
}

func issueLiveSnapshotStates(entries []issueLiveSnapshotEntry) map[string]issueLiveSnapshotState {
	states := make(map[string]issueLiveSnapshotState, len(entries))
	for _, entry := range entries {
		states[entry.Key] = issueLiveSnapshotState{BeforeKey: entry.BeforeKey, Hash: entry.Hash}
	}
	return states
}

func issueLiveServerStates(entries []issueLiveSnapshotEntry, excluded map[string]struct{}) []issueLiveServerState {
	states := make([]issueLiveServerState, 0, len(entries))
	for _, entry := range entries {
		if _, skip := excluded[entry.Key]; skip {
			continue
		}
		states = append(states, issueLiveServerState{
			Key:             entry.Key,
			BeforeKey:       entry.BeforeKey,
			Hash:            issueLiveHashString(entry.Hash),
			ContentVersion:  entry.ContentVersion,
			ReactionState:   entry.ReactionState,
			AttachmentState: entry.AttachmentState,
		})
	}
	return states
}

func issueLiveDecodeClientStates(states []issueLiveClientState) (map[string]issueLiveSnapshotState, error) {
	if len(states) > 10000 {
		return nil, errors.New("too many issue live resume states")
	}
	decoded := make(map[string]issueLiveSnapshotState, len(states))
	for _, state := range states {
		if state.Key == "" {
			continue
		}
		hashBytes, err := hex.DecodeString(state.Hash)
		if err != nil || len(hashBytes) != sha256.Size {
			return nil, fmt.Errorf("invalid issue live hash for %q", state.Key)
		}
		var hash [sha256.Size]byte
		copy(hash[:], hashBytes)
		decoded[state.Key] = issueLiveSnapshotState{BeforeKey: state.BeforeKey, Hash: hash}
	}
	return decoded, nil
}

func issueLiveInitialSnapshotStates(clientStates []issueLiveClientState, entries []issueLiveSnapshotEntry) map[string]issueLiveSnapshotState {
	serverEntries := make(map[string]issueLiveSnapshotEntry, len(entries))
	for _, entry := range entries {
		serverEntries[entry.Key] = entry
	}

	states := make(map[string]issueLiveSnapshotState, len(clientStates))
	for _, clientState := range clientStates {
		if clientState.Key == "" {
			continue
		}
		state := issueLiveSnapshotState{BeforeKey: clientState.BeforeKey}
		if entry, exists := serverEntries[clientState.Key]; exists &&
			clientState.ContentVersion == entry.ContentVersion &&
			clientState.ReactionState == entry.ReactionState &&
			clientState.AttachmentState == entry.AttachmentState {
			state.Hash = entry.Hash
		}
		states[clientState.Key] = state
	}
	return states
}

func issueLiveDiffSnapshot(previous map[string]issueLiveSnapshotState, entries []issueLiveSnapshotEntry) ([]issueLiveOperation, map[string]issueLiveSnapshotState) {
	current := issueLiveSnapshotStates(entries)
	operations := make([]issueLiveOperation, 0, len(entries))

	for _, entry := range entries {
		oldState, exists := previous[entry.Key]
		if !exists || oldState.Hash != entry.Hash || oldState.BeforeKey != entry.BeforeKey {
			operations = append(operations, issueLiveOperation{
				Action:    "upsert",
				Key:       entry.Key,
				BeforeKey: entry.BeforeKey,
				HTML:      entry.HTML,
				Hash:      issueLiveHashString(entry.Hash),
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

func issueLiveReadResume(ctx stdcontext.Context, conn *websocket.Conn) ([]issueLiveClientState, bool, error) {
	readCtx, cancel := stdcontext.WithTimeout(ctx, issueLiveResumeTimeout)
	defer cancel()
	messageType, payload, err := conn.Read(readCtx)
	if err != nil {
		return nil, false, err
	}
	if messageType != websocket.MessageText {
		return nil, false, errors.New("issue live resume message must be text")
	}

	var message issueLiveClientMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return nil, false, fmt.Errorf("decode issue live resume message: %w", err)
	}
	if message.Type != "resume" {
		return nil, false, fmt.Errorf("unexpected first issue live message %q", message.Type)
	}
	if len(message.States) > 10000 {
		return nil, false, errors.New("too many issue live resume states")
	}
	return message.States, message.Initialized, nil
}

func issueLiveReadLoop(ctx stdcontext.Context, cancel stdcontext.CancelFunc, conn *websocket.Conn, refresh chan<- struct{}) {
	defer cancel()
	for {
		messageType, payload, err := conn.Read(ctx)
		if err != nil {
			return
		}
		if messageType != websocket.MessageText {
			continue
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
// a WebSocket connection. Reconnects resume from content hashes, while a first
// connection reconciles the page's lightweight structural state before storing
// its baseline. Rendering is event-driven with a low-frequency safety refresh.
func IssueLive(ctx *context.Context) {
	connectionCtx, cancel := stdcontext.WithCancel(ctx.Req.Context())
	defer cancel()
	stopShutdownCancel := stdcontext.AfterFunc(graceful.GetManager().ShutdownContext(), cancel)
	defer stopShutdownCancel()
	ctx.Base.RequestContext = reqctx.FromContext(connectionCtx)
	ctx.Req = ctx.Req.WithContext(connectionCtx)

	refresh := make(chan struct{}, 1)
	unregisterRefresh := registerIssueLiveRefresh(ctx.Repo.Repository.ID, ctx.PathParamInt64("index"), refresh)
	defer unregisterRefresh()

	initialHTML, err := prepareIssueLiveSnapshot(ctx)
	if err != nil {
		ctx.ServerError("prepareIssueLiveSnapshot", err)
		return
	}
	initialEntries, err := issueLiveParseSnapshot(initialHTML)
	if err != nil {
		ctx.ServerError("issueLiveParseSnapshot", err)
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
	conn.SetReadLimit(issueLiveReadLimit)

	clientStateList, initialized, err := issueLiveReadResume(connectionCtx, conn)
	if err != nil {
		if websocket.CloseStatus(err) == -1 && connectionCtx.Err() == nil {
			log.Debug("Unable to read issue live resume state: %v", err)
		}
		return
	}

	var (
		sequence       uint64
		operations     []issueLiveOperation
		previous       map[string]issueLiveSnapshotState
		baselineStates []issueLiveServerState
	)
	if initialized {
		clientStates, err := issueLiveDecodeClientStates(clientStateList)
		if err != nil {
			log.Debug("Unable to decode issue live resume state: %v", err)
			return
		}
		operations, previous = issueLiveDiffSnapshot(clientStates, initialEntries)
	} else {
		initialStates := issueLiveInitialSnapshotStates(clientStateList, initialEntries)
		operations, previous = issueLiveDiffSnapshot(initialStates, initialEntries)

		changedKeys := make(map[string]struct{}, len(operations))
		for _, operation := range operations {
			if operation.Action == "upsert" {
				changedKeys[operation.Key] = struct{}{}
			}
		}
		baselineStates = issueLiveServerStates(initialEntries, changedKeys)
	}
	if len(operations) > 0 {
		sequence++
		if err := issueLiveWrite(connectionCtx, conn, issueLiveServerMessage{
			Type:       "timeline",
			Sequence:   sequence,
			Operations: operations,
		}); err != nil {
			return
		}
	}
	if baselineStates != nil {
		// Send the baseline after the initial operations. WebSocket ordering lets
		// the browser commit it only after those DOM changes have been applied.
		if err := issueLiveWrite(connectionCtx, conn, issueLiveServerMessage{
			Type:   "baseline",
			States: baselineStates,
		}); err != nil {
			return
		}
	}

	go issueLiveReadLoop(connectionCtx, cancel, conn, refresh)

	refreshTicker := time.NewTicker(issueLiveSafetyRefreshInterval)
	defer refreshTicker.Stop()
	pingTicker := time.NewTicker(issueLivePingInterval)
	defer pingTicker.Stop()

	sendSnapshot := func(rendered string) error {
		entries, err := issueLiveParseSnapshot(rendered)
		if err != nil {
			return err
		}
		operations, current := issueLiveDiffSnapshot(previous, entries)
		previous = current
		if len(operations) == 0 {
			return nil
		}

		sequence++
		return issueLiveWrite(connectionCtx, conn, issueLiveServerMessage{
			Type:       "timeline",
			Sequence:   sequence,
			Operations: operations,
		})
	}

	for {
		select {
		case <-connectionCtx.Done():
			return
		case <-refreshTicker.C:
		case <-refresh:
			// Collapse a burst of local changes into one template render.
			for {
				select {
				case <-refresh:
				default:
					goto refreshTimeline
				}
			}
		case <-pingTicker.C:
			pingCtx, pingCancel := stdcontext.WithTimeout(connectionCtx, issueLiveWriteTimeout)
			err := conn.Ping(pingCtx)
			pingCancel()
			if err != nil {
				return
			}
			continue
		}

	refreshTimeline:
		rendered, err := prepareIssueLiveSnapshot(ctx)
		if err != nil {
			if connectionCtx.Err() != nil {
				return
			}
			log.Warn("Unable to refresh live timeline for %s: %v", ctx.Req.URL.Path, err)
			continue
		}
		if err := sendSnapshot(rendered); err != nil {
			if websocket.CloseStatus(err) == -1 && connectionCtx.Err() == nil {
				log.Debug("Unable to write issue live snapshot: %v", err)
			}
			return
		}
	}
}
