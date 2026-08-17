# Integrated mailbox server

This tree can host user mailboxes directly in the Gitea database and exposes them through the Gitea web UI and IMAP. SMTP provides Internet-facing local delivery plus authenticated submission. Mail sent to non-local domains is relayed through the existing `[mailer]` transport so the existing SMTP/sendmail policy, credentials and upstream delivery infrastructure remain authoritative.

## Configuration

```ini
[mailbox]
ENABLED = true
DOMAIN = git.example.com
HOSTNAME = mail.git.example.com
WEB_ENABLED = true
SMTP_LISTEN = :25
SMTP_SUBMISSION_LISTEN = :587
SMTPS_LISTEN = :465
IMAP_LISTEN = :143
IMAPS_LISTEN = :993
TLS_CERT_FILE = /etc/gitea/mail/fullchain.pem
TLS_KEY_FILE = /etc/gitea/mail/privkey.pem
ALLOW_INSECURE_AUTH = false
RELAY_ENABLED = true
MAX_MESSAGE_SIZE = 26214400
MAX_RECIPIENTS = 100
DEFAULT_QUOTA = 0

[email.incoming]
ENABLED = true
LOCAL_DELIVERY = true
REPLY_TO_ADDRESS = incoming+%{token}@git.example.com
```

With `ALLOW_INSECURE_AUTH = false`, authenticated SMTP submission and IMAP require a TLS certificate/key pair. Leave a listener empty to disable it. Ports 465 and 993 are implicit TLS; 587 and 143 support STARTTLS when TLS is configured.

Each active individual Gitea account owns `<username>@DOMAIN`. Activated Gitea email addresses on the hosted domain are also accepted as inbound aliases. Additional aliases can be managed at `/mail/settings`.

## Existing Gitea mail integration

When `[mailer]` is enabled, Gitea-generated messages are partitioned before sending. Recipients hosted by `[mailbox] DOMAIN` are written directly to their local mailbox; other recipients continue through the configured Gitea mailer transport.

When `[email.incoming] LOCAL_DELIVERY = true`, tokenized reply-by-email addresses are consumed directly by the integrated SMTP listener. The existing incoming-mail token decoder and issue/pull-request handlers are reused; the external IMAP polling loop is disabled.

## DNS

For Internet delivery, publish an MX record for `DOMAIN` that resolves to `HOSTNAME`, and publish A/AAAA records for `HOSTNAME`. Configure PTR/rDNS and the normal sender-authentication records required by your outbound relay/provider as appropriate.

## Storage

The feature registers four isolated XORM tables:

- `mailbox_message`
- `mailbox_attachment`
- `mailbox_folder`
- `mailbox_alias`

The raw RFC 5322 message is retained for IMAP and `.eml` download. Parsed envelope/body fields are denormalized for the web UI and search. Attachments are extracted into the mailbox attachment table for authenticated downloads.

## Protocol and security scope

The integrated server implements the mailbox-facing SMTP/ESMTP path (including STARTTLS, AUTH PLAIN/LOGIN, local recipient validation, authenticated relay, size/recipient limits and null reverse paths) and an IMAP4 server backed by the same database storage. HTML mail is sanitized before rendering in the authenticated web UI.

Remote-domain outbound delivery deliberately uses the existing `[mailer]` transport rather than implementing DNS MX resolution and direct-to-MX queueing. DKIM signing, SPF/DMARC evaluation, reputation/greylisting, antivirus and spam filtering are therefore expected to be supplied by the configured outbound/inbound edge when those controls are required for an Internet-exposed production deployment.
