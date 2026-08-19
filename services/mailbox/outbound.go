// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package mailbox

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	mailbox_model "gitea.dev/models/mailbox"
	user_model "gitea.dev/models/user"
	"gitea.dev/modules/log"
	"gitea.dev/modules/setting"
	"gitea.dev/modules/timeutil"

	"github.com/emersion/go-smtp"
)

const (
	outboundDialTimeout = 30 * time.Second
	outboundLookupLimit = 10 * time.Second
	outboundBatchSize   = 32
)

// deliveryError separates a destination that may succeed later from one that
// never will. Only permanent failures produce a bounce.
type deliveryError struct {
	permanent bool
	code      int
	err       error
}

func (e *deliveryError) Error() string { return e.err.Error() }
func (e *deliveryError) Unwrap() error { return e.err }

func temporaryFailure(code int, format string, args ...any) *deliveryError {
	return &deliveryError{code: code, err: fmt.Errorf(format, args...)}
}

func permanentFailure(code int, format string, args ...any) *deliveryError {
	return &deliveryError{permanent: true, code: code, err: fmt.Errorf(format, args...)}
}

// classifySMTPError maps a reply from the remote MTA onto our retry decision.
// 5xx is permanent, 4xx is a deferral, and anything else (a dropped connection,
// a DNS hiccup) is treated as temporary so the message is retried.
func classifySMTPError(err error) *deliveryError {
	if err == nil {
		return nil
	}
	var smtpErr *smtp.SMTPError
	if errors.As(err, &smtpErr) {
		if smtpErr.Code >= 500 && smtpErr.Code < 600 {
			return permanentFailure(smtpErr.Code, "%w", err)
		}
		return temporaryFailure(smtpErr.Code, "%w", err)
	}
	return temporaryFailure(0, "%w", err)
}

// lookupMailExchangers returns the hosts to try for a domain, most preferred
// first. RFC 5321 section 5.1: a domain with no MX record falls back to its
// address record, and a single "." MX explicitly refuses mail.
func lookupMailExchangers(ctx context.Context, domain string) ([]string, error) {
	domain = normalizeDomain(domain)
	if domain == "" {
		return nil, permanentFailure(0, "invalid recipient domain")
	}
	ctx, cancel := context.WithTimeout(ctx, outboundLookupLimit)
	defer cancel()

	records, err := net.DefaultResolver.LookupMX(ctx, domain)
	if err != nil {
		var dnsErr *net.DNSError
		// A domain that resolves to nothing at all is a permanent failure; a
		// timeout or server failure is not.
		if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
			return implicitMailExchanger(ctx, domain)
		}
		if errors.As(err, &dnsErr) && dnsErr.IsTemporary {
			return nil, temporaryFailure(0, "MX lookup for %s: %w", domain, err)
		}
		// Go reports "no MX record" as a not-found error on most resolvers, but
		// some return an empty answer instead; fall back rather than defer.
		return implicitMailExchanger(ctx, domain)
	}
	if len(records) == 0 {
		return implicitMailExchanger(ctx, domain)
	}

	return sortMailExchangers(domain, records)
}

// sortMailExchangers orders MX hosts by preference and rejects a null MX.
func sortMailExchangers(domain string, records []*net.MX) ([]string, error) {
	sorted := make([]*net.MX, len(records))
	copy(sorted, records)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Pref < sorted[j].Pref })

	hosts := make([]string, 0, len(sorted))
	for _, record := range sorted {
		host := strings.TrimSuffix(strings.TrimSpace(record.Host), ".")
		if host == "" {
			// A null MX (RFC 7505) states the domain accepts no mail at all.
			return nil, permanentFailure(0, "domain %s does not accept mail (null MX)", domain)
		}
		hosts = append(hosts, host)
	}
	if len(hosts) == 0 {
		return nil, permanentFailure(0, "domain %s has no usable mail exchanger", domain)
	}
	return hosts, nil
}

func implicitMailExchanger(ctx context.Context, domain string) ([]string, error) {
	addrs, err := net.DefaultResolver.LookupHost(ctx, domain)
	if err != nil {
		var dnsErr *net.DNSError
		if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
			return nil, permanentFailure(0, "domain %s has no MX or address record", domain)
		}
		return nil, temporaryFailure(0, "address lookup for %s: %w", domain, err)
	}
	if len(addrs) == 0 {
		return nil, permanentFailure(0, "domain %s has no MX or address record", domain)
	}
	return []string{domain}, nil
}

// outboundTLSConfig implements RFC 7435 opportunistic security. MX hosts very
// often present certificates that do not match the name we looked up, so
// requiring a valid chain by default would break normal delivery; encryption
// without authentication is still strictly better than cleartext. Set
// OUTBOUND_REQUIRE_TLS to demand a verified chain instead.
func outboundTLSConfig(host string) *tls.Config {
	return &tls.Config{
		ServerName:         host,
		InsecureSkipVerify: !setting.MailboxServer.OutboundRequireTLS, //nolint:gosec // opportunistic TLS, see comment
		MinVersion:         tls.VersionTLS12,
	}
}

// deliverToHost runs one SMTP conversation. A rejection of an individual
// recipient is reported, but the message is still delivered to the rest.
func deliverToHost(ctx context.Context, host, from string, recipients []string, raw []byte) error {
	client, err := connectToHost(ctx, host)
	if err != nil {
		return err
	}
	defer client.Close()

	if err := client.Mail(from, nil); err != nil {
		return classifySMTPError(err)
	}
	accepted := 0
	var lastRcptErr error
	for _, recipient := range recipients {
		if err := client.Rcpt(recipient, nil); err != nil {
			lastRcptErr = err
			log.Warn("Mailbox outbound: %s rejected recipient %s: %v", host, recipient, err)
			continue
		}
		accepted++
	}
	if accepted == 0 {
		if lastRcptErr != nil {
			return classifySMTPError(lastRcptErr)
		}
		return permanentFailure(0, "no recipients accepted by %s", host)
	}

	writer, err := client.Data()
	if err != nil {
		return classifySMTPError(err)
	}
	if _, err := writer.Write(raw); err != nil {
		return temporaryFailure(0, "write message to %s: %w", host, err)
	}
	if err := writer.Close(); err != nil {
		return classifySMTPError(err)
	}
	_ = client.Quit()
	return nil
}

// connectToHost opens an SMTP session, preferring STARTTLS. go-smtp only exposes
// the upgrade through NewClientStartTLS, which greets as "localhost" and fails
// outright when the peer has no STARTTLS, so a peer without it costs a second
// connection. The EHLO that names this host is the one sent inside the TLS
// session, which is the greeting that governs the mail transaction.
func connectToHost(ctx context.Context, host string) (*smtp.Client, error) {
	conn, err := dialHost(ctx, host)
	if err != nil {
		return nil, err
	}
	client, tlsErr := smtp.NewClientStartTLS(conn, outboundTLSConfig(host))
	if tlsErr == nil {
		if err := client.Hello(setting.MailboxServer.OutboundHelo); err != nil {
			client.Close()
			return nil, classifySMTPError(err)
		}
		return client, nil
	}
	if setting.MailboxServer.OutboundRequireTLS {
		return nil, temporaryFailure(0, "STARTTLS with %s failed and OUTBOUND_REQUIRE_TLS is set: %w", host, tlsErr)
	}

	log.Debug("Mailbox outbound: %s has no usable STARTTLS, falling back to cleartext: %v", host, tlsErr)
	conn, err = dialHost(ctx, host)
	if err != nil {
		return nil, err
	}
	client = smtp.NewClient(conn)
	if err := client.Hello(setting.MailboxServer.OutboundHelo); err != nil {
		client.Close()
		return nil, classifySMTPError(err)
	}
	return client, nil
}

func dialHost(ctx context.Context, host string) (net.Conn, error) {
	addr := net.JoinHostPort(host, "25")
	dialer := &net.Dialer{Timeout: outboundDialTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, temporaryFailure(0, "connect to %s: %w", addr, err)
	}
	return conn, nil
}

// deliverDirect tries each MX in preference order. A permanent rejection stops
// the walk, because a lower-priority host would reject it for the same reason.
func deliverDirect(ctx context.Context, domain, from string, recipients []string, raw []byte) error {
	hosts, err := lookupMailExchangers(ctx, domain)
	if err != nil {
		return err
	}
	var lastErr error
	for _, host := range hosts {
		err := deliverToHost(ctx, host, from, recipients, raw)
		if err == nil {
			log.Info("Mailbox outbound: delivered to %s via %s (%d recipients)", domain, host, len(recipients))
			return nil
		}
		lastErr = err
		var delivery *deliveryError
		if errors.As(err, &delivery) && delivery.permanent {
			return err
		}
		log.Warn("Mailbox outbound: %s did not accept mail for %s: %v", host, domain, err)
	}
	if lastErr == nil {
		lastErr = temporaryFailure(0, "no mail exchanger available for %s", domain)
	}
	return lastErr
}

// QueueOutbound stores remote recipients for delivery, grouped by destination
// domain. Delivery itself happens on the retry loop so a slow or unreachable
// destination never blocks the SMTP session or the web request that produced it.
func QueueOutbound(ctx context.Context, userID int64, from string, recipients []string, raw []byte) error {
	byDomain, err := groupByDomain(recipients)
	if err != nil {
		return err
	}
	for domain, domainRecipients := range byDomain {
		out := &mailbox_model.Outbound{
			UserID:      userID,
			Domain:      domain,
			FromAddress: from,
			Raw:         raw,
			NextAttempt: timeutil.TimeStampNow(),
		}
		out.SetRecipients(domainRecipients)
		if err := mailbox_model.InsertOutbound(ctx, out); err != nil {
			return err
		}
	}
	return nil
}

// groupByDomain buckets recipients by destination domain, which is the unit one
// SMTP conversation can deliver.
func groupByDomain(recipients []string) (map[string][]string, error) {
	byDomain := make(map[string][]string)
	for _, recipient := range recipients {
		_, domain, ok := strings.Cut(strings.ToLower(strings.TrimSpace(recipient)), "@")
		if !ok || domain == "" {
			return nil, fmt.Errorf("outbound recipient has no domain: %s", recipient)
		}
		byDomain[domain] = append(byDomain[domain], recipient)
	}
	return byDomain, nil
}

// retryDelay backs off roughly like a conventional MTA: a few quick attempts,
// then widening intervals up to an hour.
func retryDelay(attempts int) time.Duration {
	switch {
	case attempts <= 1:
		return 5 * time.Minute
	case attempts == 2:
		return 15 * time.Minute
	case attempts == 3:
		return 30 * time.Minute
	default:
		return time.Hour
	}
}

func processOutbound(ctx context.Context, out *mailbox_model.Outbound) {
	recipients := out.RecipientList()
	if len(recipients) == 0 {
		_ = mailbox_model.DeleteOutbound(ctx, out.ID)
		return
	}

	err := deliverDirect(ctx, out.Domain, out.FromAddress, recipients, out.Raw)
	if err == nil {
		if err := mailbox_model.DeleteOutbound(ctx, out.ID); err != nil {
			log.Error("Mailbox outbound: cannot dequeue delivered message %d: %v", out.ID, err)
		}
		return
	}

	attempts := out.Attempts + 1
	var delivery *deliveryError
	permanent := errors.As(err, &delivery) && delivery.permanent
	expired := time.Since(out.CreatedUnix.AsTime()) > setting.MailboxServer.OutboundRetryMaxAge
	code := 0
	if delivery != nil {
		code = delivery.code
	}

	if permanent || expired {
		reason := "permanently rejected"
		if !permanent {
			reason = fmt.Sprintf("undeliverable after %s", setting.MailboxServer.OutboundRetryMaxAge)
		}
		log.Error("Mailbox outbound: giving up on %s for %v (%s): %v", out.Domain, recipients, reason, err)
		notifyOutboundFailure(ctx, out, recipients, reason, err)
		if err := mailbox_model.DeleteOutbound(ctx, out.ID); err != nil {
			log.Error("Mailbox outbound: cannot dequeue failed message %d: %v", out.ID, err)
		}
		return
	}

	next := timeutil.TimeStamp(time.Now().Add(retryDelay(attempts)).Unix())
	if err := mailbox_model.RescheduleOutbound(ctx, out.ID, attempts, code, err.Error(), next); err != nil {
		log.Error("Mailbox outbound: cannot reschedule message %d: %v", out.ID, err)
	}
}

// notifyOutboundFailure files a delivery status notice into the sender's own
// mailbox. It replaces the bounce an edge MTA would normally return, so a user
// still learns that their message never arrived.
func notifyOutboundFailure(ctx context.Context, out *mailbox_model.Outbound, recipients []string, reason string, cause error) {
	if out.UserID == 0 {
		return
	}
	user, err := user_model.GetUserByID(ctx, out.UserID)
	if err != nil {
		log.Warn("Mailbox outbound: cannot notify sender %d: %v", out.UserID, err)
		return
	}
	body := fmt.Sprintf("Delivery to the following recipients failed permanently:\r\n\r\n%s\r\n\r\nReason: %s\r\nDetail: %s\r\n",
		strings.Join(recipients, "\r\n"), reason, cause)
	notice := buildDeliveryStatusNotice(ctx, user, out, body)
	if _, err := StoreRaw(ctx, user, mailbox_model.FolderInbox, notice, false); err != nil {
		log.Error("Mailbox outbound: cannot store delivery failure notice for user %d: %v", out.UserID, err)
	}
}

func buildDeliveryStatusNotice(ctx context.Context, user *user_model.User, out *mailbox_model.Outbound, body string) []byte {
	header := fmt.Sprintf("From: Mail Delivery Subsystem <postmaster@%s>\r\n"+
		"To: %s\r\n"+
		"Subject: Undelivered mail returned to sender\r\n"+
		"Date: %s\r\n"+
		"Message-ID: <dsn-%d-%d@%s>\r\n"+
		"Auto-Submitted: auto-replied\r\n"+
		"Content-Type: text/plain; charset=utf-8\r\n"+
		"\r\n",
		Domain(), AddressForUser(ctx, user), time.Now().Format(time.RFC1123Z), out.ID, time.Now().UnixNano(), Domain())
	return []byte(header + body)
}

// RunOutboundQueue drains due deliveries until the context is cancelled.
func RunOutboundQueue(ctx context.Context) {
	ticker := time.NewTicker(setting.MailboxServer.OutboundRetryEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			drainOutboundQueue(ctx)
		}
	}
}

func drainOutboundQueue(ctx context.Context) {
	// The lease pushes a claimed row past the next tick, so a delivery that
	// outlives one interval is not started twice.
	lease := timeutil.TimeStamp(time.Now().Add(setting.MailboxServer.OutboundRetryEvery + outboundDialTimeout).Unix())
	batch, err := mailbox_model.ClaimDueOutbound(ctx, outboundBatchSize, lease)
	if err != nil {
		log.Error("Mailbox outbound: cannot read the queue: %v", err)
		return
	}
	if len(batch) == 0 {
		return
	}

	sem := make(chan struct{}, setting.MailboxServer.OutboundConcurrency)
	var wg sync.WaitGroup
	for _, out := range batch {
		if ctx.Err() != nil {
			return
		}
		wg.Add(1)
		go func(out *mailbox_model.Outbound) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			processOutbound(ctx, out)
		}(out)
	}
	wg.Wait()
}
