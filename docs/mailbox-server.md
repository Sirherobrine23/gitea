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

# Remote delivery. "direct" needs no [mailer] at all.
OUTBOUND_MODE = direct
OUTBOUND_HELO = mail.git.example.com
OUTBOUND_REQUIRE_TLS = false
OUTBOUND_RETRY_MAX_AGE = 72h
OUTBOUND_RETRY_EVERY = 5m
OUTBOUND_CONCURRENCY = 4
MAX_MESSAGE_SIZE = 26214400
MAX_RECIPIENTS = 100
DEFAULT_QUOTA = 0

# Role and fallback recipients. Both name a Gitea username and are optional.
POSTMASTER_USER = admin
CATCH_ALL_USER =

# Outbound DKIM signing. DKIM_DOMAIN defaults to DOMAIN.
DKIM_ENABLED = true
DKIM_DOMAIN = git.example.com
DKIM_SELECTOR = gitea
DKIM_PRIVATE_KEY_FILE = /etc/gitea/mail/dkim.key
DKIM_HEADER_CANONICALIZATION = relaxed
DKIM_BODY_CANONICALIZATION = relaxed

# Inbound sender authentication.
VERIFY_DKIM = true
VERIFY_SPF = true
VERIFY_DMARC = true
DMARC_ENFORCE = true
DMARC_DEFER_ON_TEMPFAIL = true
DMARC_QUARANTINE_TO_JUNK = true

[email.incoming]
ENABLED = true
LOCAL_DELIVERY = true
REPLY_TO_ADDRESS = incoming+%{token}@git.example.com
```

With `ALLOW_INSECURE_AUTH = false`, authenticated SMTP submission and IMAP require a TLS certificate/key pair. Leave a listener empty to disable it. Ports 465 and 993 are implicit TLS; 587 and 143 support STARTTLS when TLS is configured.

Each active individual Gitea account owns `<username>@DOMAIN` unless that local-part is assigned elsewhere. Activated Gitea email addresses on the hosted domain are also accepted as inbound aliases. Additional addresses are assigned by administrators at `/-/admin/mailbox`.

## Address ownership

Addresses are assigned by administrators at `/-/admin/mailbox`. Users can see which addresses reach them at `/mail/settings` but cannot create or remove any: letting a user claim an arbitrary local-part would let them claim someone else's mail identity.

The alias table is the authority for who owns a local-part. An account additionally receives mail at its own username, but only while no assignment claims that local-part.

### Renaming an account

Gitea releases a username when its owner renames, and lets the next account register it. A mail identity must not change hands that way: password resets and confirmations sent by third parties to the old address would land in a stranger's mailbox.

So on rename the old local-part is retained as a `retired` assignment bound to the original account. The consequences are:

- the account keeps receiving mail at both its old and its new address;
- an account that later registers the freed username does **not** get that address — `AddressForUser` reports no address for it, and mail to it keeps reaching the original owner;
- an administrator can see these entries on the mailbox page, marked *Former username*, and delete one to release the address deliberately.

Existing messages are stored against the account, never against a name, so nothing already delivered is affected by a rename.

## Recipient resolution

An inbound local recipient is matched in this order:

1. the tokenized `[email.incoming]` reply address, when `LOCAL_DELIVERY` is enabled;
2. an address assignment, first for the full local-part and then for the base local-part with any `+tag` stripped;
3. the Gitea username, after stripping any `+tag` sub-address;
4. an activated Gitea email address on the hosted domain;
5. `POSTMASTER_USER` for `postmaster@` and `abuse@`, which RFC 2142 requires a public domain to accept;
6. `CATCH_ALL_USER` for anything still unmatched.

With both `POSTMASTER_USER` and `CATCH_ALL_USER` empty, unknown recipients are rejected with `550 5.1.1` at RCPT time, which is the safer default for an Internet-exposed listener.

## DKIM, SPF and DMARC

Outbound messages produced by the webmail, authenticated SMTP submission, and the normal Gitea mailer path are signed with `github.com/emersion/go-msgauth/dkim` when `DKIM_ENABLED` is true. The private key can be RSA (PKCS#1 or PKCS#8 PEM) or Ed25519 (PKCS#8 PEM). Keep the private-key file restricted to the Gitea service account.

Inbound Internet SMTP delivery is evaluated before Gitea adds its own `Received` or `Authentication-Results` fields, so DKIM verification sees the original RFC 5322 wire representation. DKIM and DMARC use `github.com/emersion/go-msgauth`; SPF uses `blitiri.com.ar/go/spf`.

The receiver writes its results into a new `Authentication-Results` header. DMARC evaluation implements both accepted alignment paths:

- aligned SPF (`smtp.mailfrom`/HELO against RFC5322.From, according to `aspf`), or
- a passing aligned DKIM signature (`d=` against RFC5322.From, according to `adkim`).

DMARC policy discovery checks the exact RFC5322.From domain and then its organizational domain using the public suffix list. When the organizational-domain policy is inherited, `sp=` is honored for subdomains. `pct=` is applied to quarantine/reject enforcement.

With `DMARC_ENFORCE = true`:

- `p=reject`/`sp=reject` failure returns SMTP `550 5.7.1`;
- `p=quarantine`/`sp=quarantine` failure is accepted into `Junk` when `DMARC_QUARANTINE_TO_JUNK = true`;
- a temporary DNS/authentication failure that prevents DMARC evaluation returns `451 4.7.5` when `DMARC_DEFER_ON_TEMPFAIL = true`;
- `p=none` records are recorded but not rejected/quarantined.

DMARC-quarantined mail is not dispatched into Gitea's tokenized reply-by-email action handler. This prevents a message classified for quarantine from mutating an issue or pull request.

### DNS records

At minimum publish the MX/A/AAAA records for inbound delivery and the sender-authentication records appropriate to the domain. For example, with selector `gitea` the DKIM public key is published at:

```text
gitea._domainkey.git.example.com. TXT "v=DKIM1; k=rsa; p=..."
```

SPF and DMARC are normal DNS TXT records, for example:

```text
git.example.com.        TXT "v=spf1 mx -all"
_dmarc.git.example.com. TXT "v=DMARC1; p=reject; adkim=r; aspf=r; pct=100"
```

The exact SPF policy must describe the systems that really emit mail for your domain. If `[mailer]` relays through a separate provider, that provider must be represented in SPF as appropriate. Configure PTR/rDNS for the SMTP egress host as well.

## Outbound delivery

`OUTBOUND_MODE` decides how mail for remote domains leaves the instance.

With `direct` (the default) the server is a complete MTA and `[mailer]` is not required at all. Remote recipients are grouped by destination domain and queued in `mailbox_outbound`, then delivered by a background worker that:

- resolves the domain's MX records and tries them in preference order, falling back to the domain's A/AAAA record when it publishes no MX (RFC 5321 5.1) and refusing a null MX (RFC 7505);
- upgrades the connection with STARTTLS when the peer offers it. Certificates are *not* verified by default, because MX hosts routinely present names that do not match the record used to reach them; this is the opportunistic model of RFC 7435, and encryption without authentication is still better than cleartext. Set `OUTBOUND_REQUIRE_TLS = true` to demand a verified chain and refuse to deliver without one;
- treats a 5xx reply as permanent and a 4xx reply, a DNS temporary failure or a dropped connection as a deferral;
- retries deferrals with a widening backoff (5m, 15m, 30m, then hourly) until `OUTBOUND_RETRY_MAX_AGE`, after which the message is given up on.

When a message is given up on, a delivery status notice is filed into the sender's own mailbox, which stands in for the bounce an edge MTA would normally return. Mail this instance did not originate has no local sender to notify and is only logged.

Queue rows are leased before they are handed to a worker, so overlapping runs — or a second Gitea instance on the same database — cannot deliver the same message twice.

With `relay`, remote recipients are handed to the `[mailer]` transport instead. Use it behind a smarthost, or when the host cannot open outbound port 25, which many cloud providers and every residential ISP block.

Direct mode requires the operator to look like a legitimate sender: a static egress IP with forward-confirmed reverse DNS matching `OUTBOUND_HELO`, an SPF record covering that IP, and DKIM signing enabled. Without those, large providers will reject or spam-file the mail regardless of how correct the SMTP conversation is.

## Existing Gitea mail integration

Gitea-generated messages are partitioned before sending. Recipients hosted by `[mailbox] DOMAIN` are written directly to their local mailbox; other recipients follow `OUTBOUND_MODE`. DKIM signing happens before this partition, so the same signed RFC 5322 message is used for both local and remote copies.

Because the integrated server can carry Gitea's own notifications, `[mailer]` may be omitted entirely: password resets, registration confirmations, team invites and notification mail all work with `[mailbox]` alone.

When `[email.incoming] LOCAL_DELIVERY = true`, tokenized reply-by-email addresses are consumed directly by the integrated SMTP listener. The existing incoming-mail token decoder and issue/pull-request handlers are reused; the external IMAP polling loop is disabled.

## Storage

The feature registers four isolated XORM tables:

- `mailbox_message`
- `mailbox_attachment`
- `mailbox_folder`
- `mailbox_alias`

The raw RFC 5322 message is retained for IMAP and `.eml` download. Parsed envelope/body fields are denormalized for the web UI and search. Attachments are extracted into the mailbox attachment table for authenticated downloads.

## Protocol and security scope

## Libraries

The protocol layers are provided by the `github.com/emersion` mail stack rather than hand-written:

| Concern | Library |
| --- | --- |
| SMTP/ESMTP server (STARTTLS, AUTH, SIZE, line and message limits, DATA transparency) | `github.com/emersion/go-smtp` |
| SASL mechanisms | `github.com/emersion/go-sasl` |
| IMAP4rev1 server | `github.com/emersion/go-imap` |
| DKIM, DMARC, Authentication-Results | `github.com/emersion/go-msgauth` |
| MIME parsing | `github.com/emersion/go-message`, `github.com/jhillyerd/enmime` |
| SPF | `blitiri.com.ar/go/spf` |

Gitea supplies only the parts that are specific to it: the session backends, recipient resolution against Gitea accounts and aliases, delivery into the database, and the sender-authentication policy. `AUTH LOGIN` is the one protocol detail implemented here, because `go-sasl` ships only a client for that mechanism.

## Limitations

The integrated server covers the mailbox-facing SMTP path (local recipient validation, authenticated relay and null reverse paths) and an IMAP4 server backed by the same database storage. HTML mail is sanitized before rendering in the authenticated web UI.

MX resolution, opportunistic TLS, deferral retries and delivery status notices are native. Not implemented: full RFC 3464 DSN bodies (the notice is a plain-text message), inbound greylisting, reputation scoring, antivirus and content-spam filtering. A production Internet deployment still needs correct DNS, abuse controls and any desired spam/virus filtering at the boundary.
