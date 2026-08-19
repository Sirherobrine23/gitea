// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package mailbox

import (
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"

	mailbox_model "gitea.dev/models/mailbox"
	"gitea.dev/modules/httplib"
	"gitea.dev/modules/markup"
	"gitea.dev/modules/setting"
	"gitea.dev/modules/templates"
	"gitea.dev/services/context"
	mailbox_service "gitea.dev/services/mailbox"
)

const (
	tplList     templates.TplName = "mailbox/list"
	tplView     templates.TplName = "mailbox/view"
	tplCompose  templates.TplName = "mailbox/compose"
	tplSettings templates.TplName = "mailbox/settings"
)

func prepareCommon(ctx *context.Context) bool {
	if !setting.MailboxServer.Enabled || !setting.MailboxServer.WebEnabled {
		ctx.NotFound(nil)
		return false
	}
	if err := mailbox_model.EnsureSystemFolders(ctx, ctx.Doer.ID); err != nil {
		ctx.ServerError("EnsureSystemFolders", err)
		return false
	}
	folders, err := mailbox_model.ListFolders(ctx, ctx.Doer.ID, false)
	if err != nil {
		ctx.ServerError("ListFolders", err)
		return false
	}
	unread, err := mailbox_model.UnreadCount(ctx, ctx.Doer.ID)
	if err != nil {
		ctx.ServerError("UnreadCount", err)
		return false
	}
	ctx.Data["Title"] = ctx.Locale.TrString("mailbox.title")
	ctx.Data["PageIsMailbox"] = true
	ctx.Data["MailboxFolders"] = folders
	ctx.Data["MailboxUnread"] = unread
	ctx.Data["MailboxAddress"] = mailbox_service.AddressForUser(ctx, ctx.Doer)
	return true
}

func List(ctx *context.Context) {
	if !prepareCommon(ctx) {
		return
	}
	folder := mailbox_model.NormalizeFolder(ctx.FormString("folder"))
	if folder == "" {
		folder = mailbox_model.FolderInbox
	}
	if exists, err := mailbox_model.FolderExists(ctx, ctx.Doer.ID, folder); err != nil {
		ctx.ServerError("FolderExists", err)
		return
	} else if !exists {
		ctx.NotFound(nil)
		return
	}
	page := max(1, ctx.FormInt("page"))
	const pageSize = 30
	messages, total, err := mailbox_model.ListMessages(ctx, ctx.Doer.ID, folder, ctx.FormString("q"), pageSize, (page-1)*pageSize)
	if err != nil {
		ctx.ServerError("ListMessages", err)
		return
	}
	pager := context.NewPagination(total, pageSize, page, 5)
	pager.AddParamFromRequest(ctx.Req)
	ctx.Data["Messages"] = messages
	ctx.Data["MailboxFolder"] = folder
	ctx.Data["MailboxQuery"] = ctx.FormString("q")
	ctx.Data["Page"] = pager
	ctx.HTML(http.StatusOK, tplList)
}

func View(ctx *context.Context) {
	if !prepareCommon(ctx) {
		return
	}
	msg, err := mailbox_model.GetMessage(ctx, ctx.Doer.ID, ctx.PathParamInt64("id"))
	if err != nil {
		ctx.NotFound(err)
		return
	}
	if !msg.Seen {
		if err := mailbox_model.MarkRead(ctx, ctx.Doer.ID, msg.ID, true); err != nil {
			ctx.ServerError("MarkRead", err)
			return
		}
		msg.Seen = true
	}
	attachments, err := mailbox_model.GetAttachments(ctx, ctx.Doer.ID, msg.ID)
	if err != nil {
		ctx.ServerError("GetAttachments", err)
		return
	}
	ctx.Data["Message"] = msg
	ctx.Data["Attachments"] = attachments
	ctx.Data["MailboxFolder"] = msg.Folder
	if msg.HTMLBody != "" {
		ctx.Data["MailboxHTMLBody"] = markup.Sanitize(msg.HTMLBody)
	}
	ctx.HTML(http.StatusOK, tplView)
}

func Compose(ctx *context.Context) {
	if !prepareCommon(ctx) {
		return
	}
	ctx.Data["ComposeTo"] = ctx.FormString("to")
	ctx.Data["ComposeSubject"] = ctx.FormString("subject")
	if replyID := ctx.FormInt64("reply"); replyID > 0 {
		msg, err := mailbox_model.GetMessage(ctx, ctx.Doer.ID, replyID)
		if err == nil {
			to := msg.ReplyTo
			if strings.TrimSpace(to) == "" {
				to = msg.FromAddress
			}
			ctx.Data["ComposeTo"] = to
			subject := msg.Subject
			if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(subject)), "re:") {
				subject = "Re: " + subject
			}
			ctx.Data["ComposeSubject"] = subject
		}
	}
	ctx.HTML(http.StatusOK, tplCompose)
}

func ComposePost(ctx *context.Context) {
	if !prepareCommon(ctx) {
		return
	}
	to, err := mailbox_service.ParseRecipientList(ctx.FormString("to"))
	if err != nil {
		composeError(ctx, fmt.Errorf("invalid To address: %w", err))
		return
	}
	cc, err := mailbox_service.ParseRecipientList(ctx.FormString("cc"))
	if err != nil {
		composeError(ctx, fmt.Errorf("invalid Cc address: %w", err))
		return
	}
	bcc, err := mailbox_service.ParseRecipientList(ctx.FormString("bcc"))
	if err != nil {
		composeError(ctx, fmt.Errorf("invalid Bcc address: %w", err))
		return
	}
	attachments, err := readComposeAttachments(ctx)
	if err != nil {
		composeError(ctx, err)
		return
	}
	_, err = mailbox_service.ComposeAndSend(ctx, ctx.Doer, to, cc, bcc, ctx.FormString("subject"), ctx.FormString("body"), attachments)
	if err != nil {
		composeError(ctx, err)
		return
	}
	ctx.Flash.Success("Email sent")
	ctx.Redirect(setting.AppSubURL + "/mail?folder=Sent")
}

func composeError(ctx *context.Context, err error) {
	ctx.Data["ComposeTo"] = ctx.FormString("to")
	ctx.Data["ComposeCc"] = ctx.FormString("cc")
	ctx.Data["ComposeBcc"] = ctx.FormString("bcc")
	ctx.Data["ComposeSubject"] = ctx.FormString("subject")
	ctx.Data["ComposeBody"] = ctx.FormString("body")
	ctx.Data["ComposeError"] = err.Error()
	ctx.HTML(http.StatusUnprocessableEntity, tplCompose)
}

func readComposeAttachments(ctx *context.Context) ([]mailbox_service.ComposeAttachment, error) {
	if ctx.Req.MultipartForm == nil {
		return nil, nil
	}
	files := ctx.Req.MultipartForm.File["attachments"]
	attachments := make([]mailbox_service.ComposeAttachment, 0, len(files))
	var total int64
	limit := setting.MailboxServer.MaxMessageSize
	if limit <= 0 {
		limit = 100 * 1024 * 1024
	}
	for _, header := range files {
		file, err := header.Open()
		if err != nil {
			return nil, err
		}
		content, readErr := io.ReadAll(io.LimitReader(file, limit+1))
		closeErr := file.Close()
		if readErr != nil {
			return nil, readErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		total += int64(len(content))
		if int64(len(content)) > limit || total > limit {
			return nil, fmt.Errorf("attachments exceed the configured message size limit")
		}
		contentType := header.Header.Get("Content-Type")
		if contentType == "" {
			contentType = http.DetectContentType(content)
		}
		attachments = append(attachments, mailbox_service.ComposeAttachment{Filename: header.Filename, ContentType: contentType, Content: content})
	}
	return attachments, nil
}

func Action(ctx *context.Context) {
	if !prepareCommon(ctx) {
		return
	}
	id := ctx.PathParamInt64("id")
	msg, err := mailbox_model.GetMessage(ctx, ctx.Doer.ID, id)
	if err != nil {
		ctx.NotFound(err)
		return
	}
	switch ctx.FormString("action") {
	case "read":
		err = mailbox_model.MarkRead(ctx, ctx.Doer.ID, id, true)
	case "unread":
		err = mailbox_model.MarkRead(ctx, ctx.Doer.ID, id, false)
	case "flag":
		err = mailbox_model.SetFlagged(ctx, ctx.Doer.ID, id, true)
	case "unflag":
		err = mailbox_model.SetFlagged(ctx, ctx.Doer.ID, id, false)
	case "delete":
		err = mailbox_model.DeleteMessage(ctx, ctx.Doer.ID, id)
	case "move":
		err = mailbox_model.MoveMessage(ctx, ctx.Doer.ID, id, ctx.FormString("folder"))
	default:
		ctx.Status(http.StatusBadRequest)
		return
	}
	if err != nil {
		ctx.ServerError("MailboxAction", err)
		return
	}
	folder := msg.Folder
	if ctx.FormString("action") == "delete" && folder != mailbox_model.FolderTrash {
		folder = mailbox_model.FolderTrash
	}
	ctx.Redirect(setting.AppSubURL + "/mail?folder=" + url.QueryEscape(folder))
}

func Raw(ctx *context.Context) {
	if !setting.MailboxServer.Enabled || !setting.MailboxServer.WebEnabled {
		ctx.NotFound(nil)
		return
	}
	msg, err := mailbox_model.GetMessage(ctx, ctx.Doer.ID, ctx.PathParamInt64("id"))
	if err != nil {
		ctx.NotFound(err)
		return
	}
	ctx.Resp.Header().Set("Content-Type", "message/rfc822")
	ctx.Resp.Header().Set("Content-Disposition", httplib.EncodeContentDispositionAttachment("message.eml"))
	ctx.Resp.Header().Set("X-Content-Type-Options", "nosniff")
	_, _ = ctx.Resp.Write(msg.Raw)
}

func Attachment(ctx *context.Context) {
	if !setting.MailboxServer.Enabled || !setting.MailboxServer.WebEnabled {
		ctx.NotFound(nil)
		return
	}
	attachment, err := mailbox_model.GetAttachment(ctx, ctx.Doer.ID, ctx.PathParamInt64("attachment"))
	if err != nil {
		ctx.NotFound(err)
		return
	}
	// Make sure the attachment belongs to the message in the URL as well.
	if attachment.MessageID != ctx.PathParamInt64("id") {
		ctx.NotFound(nil)
		return
	}
	contentType := attachment.ContentType
	if mediaType, params, err := mime.ParseMediaType(contentType); err == nil {
		contentType = mime.FormatMediaType(mediaType, params)
	} else {
		contentType = "application/octet-stream"
	}
	filename := strings.TrimSpace(attachment.Filename)
	if filename == "" {
		filename = "attachment"
	}
	ctx.Resp.Header().Set("Content-Type", contentType)
	ctx.Resp.Header().Set("Content-Disposition", httplib.EncodeContentDispositionAttachment(filename))
	ctx.Resp.Header().Set("X-Content-Type-Options", "nosniff")
	_, _ = ctx.Resp.Write(attachment.Content)
}

func Settings(ctx *context.Context) {
	if !prepareCommon(ctx) {
		return
	}
	aliases, err := mailbox_model.ListAliases(ctx, ctx.Doer.ID)
	if err != nil {
		ctx.ServerError("ListAliases", err)
		return
	}
	ctx.Data["MailboxAliases"] = aliases
	ctx.Data["MailboxDomain"] = mailbox_service.Domain()
	customFolders := make([]*mailbox_model.Folder, 0)
	for _, folder := range ctx.Data["MailboxFolders"].([]*mailbox_model.Folder) {
		if !mailbox_model.IsSystemFolder(folder.Name) {
			customFolders = append(customFolders, folder)
		}
	}
	ctx.Data["MailboxCustomFolders"] = customFolders
	ctx.HTML(http.StatusOK, tplSettings)
}

func CreateFolder(ctx *context.Context) {
	if !prepareCommon(ctx) {
		return
	}
	if err := mailbox_model.CreateFolder(ctx, ctx.Doer.ID, ctx.FormString("name")); err != nil {
		ctx.Flash.Error(err.Error())
	} else {
		ctx.Flash.Success("Mail folder created")
	}
	ctx.Redirect(setting.AppSubURL + "/mail/settings")
}

func DeleteFolder(ctx *context.Context) {
	if !prepareCommon(ctx) {
		return
	}
	if err := mailbox_model.DeleteFolder(ctx, ctx.Doer.ID, ctx.FormString("name")); err != nil {
		ctx.Flash.Error(err.Error())
	} else {
		ctx.Flash.Success("Mail folder deleted")
	}
	ctx.Redirect(setting.AppSubURL + "/mail/settings")
}
