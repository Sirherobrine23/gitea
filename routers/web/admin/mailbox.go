// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package admin

import (
	"net/http"

	mailbox_model "gitea.dev/models/mailbox"
	user_model "gitea.dev/models/user"
	"gitea.dev/modules/setting"
	"gitea.dev/modules/templates"
	"gitea.dev/services/context"
	mailbox_service "gitea.dev/services/mailbox"
)

const tplMailbox templates.TplName = "admin/mailbox"

// aliasEntry pairs a stored binding with the account that owns it, so the page
// can show who receives an address without a lookup per row in the template.
type aliasEntry struct {
	Alias *mailbox_model.Alias
	Owner *user_model.User
}

func Mailbox(ctx *context.Context) {
	if !setting.MailboxServer.Enabled {
		ctx.NotFound(nil)
		return
	}
	aliases, err := mailbox_model.ListAllAliases(ctx)
	if err != nil {
		ctx.ServerError("ListAllAliases", err)
		return
	}
	entries := make([]*aliasEntry, 0, len(aliases))
	for _, alias := range aliases {
		entry := &aliasEntry{Alias: alias}
		if owner, err := user_model.GetUserByID(ctx, alias.UserID); err == nil {
			entry.Owner = owner
		}
		entries = append(entries, entry)
	}
	ctx.Data["Title"] = ctx.Tr("admin.mailbox")
	ctx.Data["PageIsAdminMailbox"] = true
	ctx.Data["MailboxAliases"] = entries
	ctx.Data["MailboxDomain"] = mailbox_service.Domain()
	ctx.HTML(http.StatusOK, tplMailbox)
}

func MailboxAddAlias(ctx *context.Context) {
	if !setting.MailboxServer.Enabled {
		ctx.NotFound(nil)
		return
	}
	owner, err := user_model.GetUserByName(ctx, ctx.FormString("username"))
	if err != nil {
		ctx.Flash.Error(ctx.Tr("admin.mailbox.alias_owner_missing"))
		ctx.Redirect(setting.AppSubURL + "/-/admin/mailbox")
		return
	}
	if err := mailbox_service.AddAlias(ctx, owner, ctx.FormString("local_part")); err != nil {
		ctx.Flash.Error(err.Error())
	} else {
		ctx.Flash.Success(ctx.Tr("admin.mailbox.alias_added"))
	}
	ctx.Redirect(setting.AppSubURL + "/-/admin/mailbox")
}

func MailboxDeleteAlias(ctx *context.Context) {
	if !setting.MailboxServer.Enabled {
		ctx.NotFound(nil)
		return
	}
	if err := mailbox_model.DeleteAliasByID(ctx, ctx.FormInt64("id")); err != nil {
		ctx.ServerError("DeleteAliasByID", err)
		return
	}
	ctx.Flash.Success(ctx.Tr("admin.mailbox.alias_removed"))
	ctx.Redirect(setting.AppSubURL + "/-/admin/mailbox")
}
