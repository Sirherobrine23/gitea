// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package mailbox

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/mail"
	"os"
	"strings"
	"sync"
	"time"

	"gitea.dev/modules/log"
	"gitea.dev/modules/setting"

	"blitiri.com.ar/go/spf"
	"github.com/emersion/go-msgauth/authres"
	"github.com/emersion/go-msgauth/dkim"
	"github.com/emersion/go-msgauth/dmarc"
	"golang.org/x/net/idna"
	"golang.org/x/net/publicsuffix"
)

const messageAuthenticationTimeout = 30 * time.Second

var (
	outboundDKIMSigner   crypto.Signer
	outboundDKIMSignerMu sync.Mutex
)

type inboundAuthAction uint8

const (
	inboundAuthAccept inboundAuthAction = iota
	inboundAuthQuarantine
	inboundAuthReject
	inboundAuthDefer
)

type inboundAuthentication struct {
	Header string
	Action inboundAuthAction
}

func initMessageAuthentication() error {
	if strings.ContainsAny(setting.MailboxServer.Hostname, "\r\n") {
		return errors.New("[mailbox] HOSTNAME contains an invalid newline")
	}
	if setting.MailboxServer.VerifyDMARC && !setting.MailboxServer.VerifyDKIM && !setting.MailboxServer.VerifySPF {
		return errors.New("[mailbox] VERIFY_DMARC requires VERIFY_DKIM and/or VERIFY_SPF")
	}
	if !setting.MailboxServer.DKIMEnabled {
		return nil
	}
	if setting.MailboxServer.DKIMDomain == "" {
		return errors.New("[mailbox] DKIM_DOMAIN must be configured when DKIM_ENABLED is true")
	}
	if setting.MailboxServer.DKIMSelector == "" {
		return errors.New("[mailbox] DKIM_SELECTOR must be configured when DKIM_ENABLED is true")
	}
	if setting.MailboxServer.DKIMPrivateKeyFile == "" {
		return errors.New("[mailbox] DKIM_PRIVATE_KEY_FILE must be configured when DKIM_ENABLED is true")
	}
	if _, err := canonicalization(setting.MailboxServer.DKIMHeaderCanonicalization); err != nil {
		return fmt.Errorf("[mailbox] DKIM_HEADER_CANONICALIZATION: %w", err)
	}
	if _, err := canonicalization(setting.MailboxServer.DKIMBodyCanonicalization); err != nil {
		return fmt.Errorf("[mailbox] DKIM_BODY_CANONICALIZATION: %w", err)
	}

	return ensureDKIMSigner()
}

func ensureDKIMSigner() error {
	if !setting.MailboxServer.DKIMEnabled {
		return nil
	}
	outboundDKIMSignerMu.Lock()
	defer outboundDKIMSignerMu.Unlock()
	if outboundDKIMSigner != nil {
		return nil
	}
	keyData, err := os.ReadFile(setting.MailboxServer.DKIMPrivateKeyFile)
	if err != nil {
		return fmt.Errorf("read DKIM private key: %w", err)
	}
	signer, err := parseDKIMPrivateKey(keyData)
	if err != nil {
		return fmt.Errorf("parse DKIM private key: %w", err)
	}
	outboundDKIMSigner = signer
	return nil
}

func parseDKIMPrivateKey(data []byte) (crypto.Signer, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, errors.New("private key is not PEM encoded")
	}

	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	if key, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		signer, ok := key.(crypto.Signer)
		if !ok {
			return nil, fmt.Errorf("PKCS#8 private key type %T does not implement crypto.Signer", key)
		}
		switch signer.Public().(type) {
		case *rsa.PublicKey, ed25519.PublicKey:
			return signer, nil
		default:
			return nil, fmt.Errorf("unsupported DKIM private key type %T (RSA and Ed25519 are supported)", signer.Public())
		}
	}
	return nil, errors.New("unsupported private key encoding; expected RSA PKCS#1 or RSA/Ed25519 PKCS#8")
}

func canonicalization(value string) (dkim.Canonicalization, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "simple":
		return dkim.CanonicalizationSimple, nil
	case "", "relaxed":
		return dkim.CanonicalizationRelaxed, nil
	default:
		return "", fmt.Errorf("unsupported value %q (expected simple or relaxed)", value)
	}
}

// SignOutboundDKIM prepends a DKIM-Signature to an RFC 5322 message when the
// integrated mailbox DKIM signer is enabled. The message bytes are otherwise
// returned unchanged.
func SignOutboundDKIM(raw []byte) ([]byte, error) {
	if !setting.MailboxServer.DKIMEnabled {
		return raw, nil
	}
	if err := ensureDKIMSigner(); err != nil {
		return nil, err
	}
	outboundDKIMSignerMu.Lock()
	signerKey := outboundDKIMSigner
	outboundDKIMSignerMu.Unlock()
	if signerKey == nil {
		return nil, errors.New("DKIM is enabled but the signer is not initialized")
	}
	headerCanon, err := canonicalization(setting.MailboxServer.DKIMHeaderCanonicalization)
	if err != nil {
		return nil, err
	}
	bodyCanon, err := canonicalization(setting.MailboxServer.DKIMBodyCanonicalization)
	if err != nil {
		return nil, err
	}

	signer, err := dkim.NewSigner(&dkim.SignOptions{
		Domain:                 setting.MailboxServer.DKIMDomain,
		Selector:               setting.MailboxServer.DKIMSelector,
		Signer:                 signerKey,
		Hash:                   crypto.SHA256,
		HeaderCanonicalization: headerCanon,
		BodyCanonicalization:   bodyCanon,
		HeaderKeys: []string{
			"From", "To", "Cc", "Reply-To", "Subject", "Date", "Message-ID",
			"MIME-Version", "Content-Type", "Content-Transfer-Encoding",
			"In-Reply-To", "References", "List-ID", "List-Unsubscribe", "List-Unsubscribe-Post",
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create DKIM signer: %w", err)
	}
	if _, err := signer.Write(raw); err != nil {
		_ = signer.Close()
		return nil, fmt.Errorf("write message to DKIM signer: %w", err)
	}
	if err := signer.Close(); err != nil {
		return nil, fmt.Errorf("finalize DKIM signature: %w", err)
	}
	sig := signer.Signature()
	out := make([]byte, 0, len(sig)+len(raw))
	out = append(out, sig...)
	out = append(out, raw...)
	return out, nil
}

func authenticateIncoming(ctx context.Context, remoteIP net.IP, helo, envelopeFrom string, raw []byte) *inboundAuthentication {
	ctx, cancel := context.WithTimeout(ctx, messageAuthenticationTimeout)
	defer cancel()

	results := make([]authres.Result, 0, 4)

	spfValue := authres.ResultNone
	spfIdentity := smtpIdentityDomain(envelopeFrom, helo)
	spfTempError := false
	if setting.MailboxServer.VerifySPF && remoteIP != nil {
		spfResult, spfErr := spf.CheckHostWithSender(remoteIP, helo, envelopeFrom, spf.WithContext(ctx))
		spfValue = spfResultValue(spfResult)
		spfTempError = spfResult == spf.TempError
		spfAuth := &authres.SPFResult{Value: spfValue, Helo: cleanAuthValue(helo)}
		if envelopeFrom != "" {
			spfAuth.From = cleanAuthValue(envelopeFrom)
		}
		if spfErr != nil && spfResult != spf.Pass {
			spfAuth.Reason = cleanAuthReason(spfErr)
		}
		results = append(results, spfAuth)
	}

	verifications := make([]*dkim.Verification, 0)
	dkimTempError := false
	if setting.MailboxServer.VerifyDKIM {
		verified, verifyErr := dkim.VerifyWithOptions(bytes.NewReader(raw), &dkim.VerifyOptions{
			LookupTXT: func(domain string) ([]string, error) {
				return net.DefaultResolver.LookupTXT(ctx, domain)
			},
			MaxVerifications: 16,
		})
		if verifyErr != nil {
			var value authres.ResultValue = authres.ResultFail
			if dkim.IsTempFail(verifyErr) {
				value = authres.ResultTempError
				dkimTempError = true
			} else if dkim.IsPermFail(verifyErr) {
				value = authres.ResultPermError
			}
			results = append(results, &authres.DKIMResult{Value: value, Reason: cleanAuthReason(verifyErr)})
		} else if len(verified) == 0 {
			results = append(results, &authres.DKIMResult{Value: authres.ResultNone})
		} else {
			verifications = verified
			for _, verification := range verified {
				var value authres.ResultValue = authres.ResultPass
				if verification.Err != nil {
					value = authres.ResultFail
					if dkim.IsTempFail(verification.Err) {
						value = authres.ResultTempError
						dkimTempError = true
					} else if dkim.IsPermFail(verification.Err) {
						value = authres.ResultPermError
					}
				}
				entry := &authres.DKIMResult{
					Value:      value,
					Domain:     cleanAuthValue(verification.Domain),
					Identifier: cleanAuthValue(verification.Identifier),
				}
				if verification.Err != nil {
					entry.Reason = cleanAuthReason(verification.Err)
				}
				results = append(results, entry)
			}
		}
	}

	auth := &inboundAuthentication{Action: inboundAuthAccept}
	fromDomain, fromErr := rfc5322FromDomain(raw)
	if setting.MailboxServer.VerifyDMARC {
		dmarcResult := &authres.DMARCResult{From: cleanAuthValue(fromDomain)}
		switch {
		case fromErr != nil:
			dmarcResult.Value = authres.ResultPermError
			dmarcResult.Reason = cleanAuthReason(fromErr)
		case fromDomain == "":
			dmarcResult.Value = authres.ResultPermError
			dmarcResult.Reason = "missing RFC5322.From domain"
		default:
			record, policyDomain, organizational, err := lookupDMARCPolicy(ctx, fromDomain)
			if err != nil {
				switch {
				case errors.Is(err, dmarc.ErrNoPolicy):
					dmarcResult.Value = authres.ResultNone
				case dmarc.IsTempFail(err):
					dmarcResult.Value = authres.ResultTempError
					dmarcResult.Reason = cleanAuthReason(err)
					if setting.MailboxServer.DMARCEnforce && setting.MailboxServer.DMARCDeferOnTempFail {
						auth.Action = inboundAuthDefer
					}
				default:
					dmarcResult.Value = authres.ResultPermError
					dmarcResult.Reason = cleanAuthReason(err)
				}
			} else {
				alignedSPF := setting.MailboxServer.VerifySPF && spfValue == authres.ResultPass && domainAligned(spfIdentity, fromDomain, record.SPFAlignment)
				alignedDKIM := false
				for _, verification := range verifications {
					if verification.Err == nil && domainAligned(verification.Domain, fromDomain, record.DKIMAlignment) {
						alignedDKIM = true
						break
					}
				}

				if alignedSPF || alignedDKIM {
					dmarcResult.Value = authres.ResultPass
				} else if spfTempError || dkimTempError {
					dmarcResult.Value = authres.ResultTempError
					dmarcResult.Reason = "SPF or DKIM evaluation had a temporary failure"
					if setting.MailboxServer.DMARCEnforce && setting.MailboxServer.DMARCDeferOnTempFail {
						auth.Action = inboundAuthDefer
					}
				} else {
					dmarcResult.Value = authres.ResultFail
					dmarcResult.Reason = fmt.Sprintf("no aligned SPF or DKIM identifier for %s", fromDomain)
					policy := record.Policy
					if organizational && record.SubdomainPolicy != "" {
						policy = record.SubdomainPolicy
					}
					if setting.MailboxServer.DMARCEnforce && dmarcSampleApplies(record, raw, remoteIP) {
						switch policy {
						case dmarc.PolicyReject:
							auth.Action = inboundAuthReject
						case dmarc.PolicyQuarantine:
							auth.Action = inboundAuthQuarantine
						}
					}
				}
				if policyDomain != "" && policyDomain != fromDomain {
					dmarcResult.Reason = strings.TrimSpace(dmarcResult.Reason + " policy-domain=" + policyDomain)
				}
			}
		}
		results = append(results, dmarcResult)
	}

	if len(results) > 0 {
		auth.Header = formatAuthenticationResults(setting.MailboxServer.Hostname, results)
	}
	return auth
}

func lookupDMARCPolicy(ctx context.Context, fromDomain string) (*dmarc.Record, string, bool, error) {
	fromDomain = normalizeDomain(fromDomain)
	if fromDomain == "" {
		return nil, "", false, dmarc.ErrNoPolicy
	}
	lookup := func(domain string) (*dmarc.Record, error) {
		return dmarc.LookupWithOptions(domain, &dmarc.LookupOptions{
			LookupTXT: func(name string) ([]string, error) {
				return net.DefaultResolver.LookupTXT(ctx, name)
			},
		})
	}
	record, err := lookup(fromDomain)
	if err == nil {
		return record, fromDomain, false, nil
	}
	if !errors.Is(err, dmarc.ErrNoPolicy) {
		return nil, fromDomain, false, err
	}
	org, orgErr := publicsuffix.EffectiveTLDPlusOne(fromDomain)
	if orgErr != nil || strings.EqualFold(org, fromDomain) {
		return nil, fromDomain, false, dmarc.ErrNoPolicy
	}
	record, err = lookup(org)
	if err != nil {
		return nil, org, true, err
	}
	return record, org, true, nil
}

func rfc5322FromDomain(raw []byte) (string, error) {
	message, err := mail.ReadMessage(bytes.NewReader(raw))
	if err != nil {
		return "", fmt.Errorf("parse RFC5322 message: %w", err)
	}
	from, err := message.Header.AddressList("From")
	if err != nil {
		return "", fmt.Errorf("parse RFC5322.From: %w", err)
	}
	if len(from) != 1 {
		return "", fmt.Errorf("RFC5322.From must contain exactly one mailbox, got %d", len(from))
	}
	_, domain, ok := strings.Cut(from[0].Address, "@")
	if !ok {
		return "", errors.New("RFC5322.From has no domain")
	}
	domain = normalizeDomain(domain)
	if domain == "" {
		return "", errors.New("RFC5322.From has an invalid domain")
	}
	return domain, nil
}

func smtpIdentityDomain(envelopeFrom, helo string) string {
	if envelopeFrom != "" {
		if parsed, err := mail.ParseAddress(envelopeFrom); err == nil {
			if _, domain, ok := strings.Cut(parsed.Address, "@"); ok {
				return normalizeDomain(domain)
			}
		}
	}
	return normalizeDomain(strings.Trim(helo, "[]"))
}

func normalizeDomain(domain string) string {
	domain = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(domain)), ".")
	if domain == "" || net.ParseIP(domain) != nil {
		return domain
	}
	ascii, err := idna.Lookup.ToASCII(domain)
	if err != nil {
		return ""
	}
	return strings.ToLower(strings.TrimSuffix(ascii, "."))
}

func domainAligned(authDomain, fromDomain string, mode dmarc.AlignmentMode) bool {
	authDomain = normalizeDomain(authDomain)
	fromDomain = normalizeDomain(fromDomain)
	if authDomain == "" || fromDomain == "" {
		return false
	}
	if mode == dmarc.AlignmentStrict {
		return authDomain == fromDomain
	}
	authOrg, authErr := publicsuffix.EffectiveTLDPlusOne(authDomain)
	fromOrg, fromErr := publicsuffix.EffectiveTLDPlusOne(fromDomain)
	if authErr != nil || fromErr != nil {
		return authDomain == fromDomain
	}
	return strings.EqualFold(authOrg, fromOrg)
}

func spfResultValue(result spf.Result) authres.ResultValue {
	switch result {
	case spf.Pass:
		return authres.ResultPass
	case spf.Fail:
		return authres.ResultFail
	case spf.SoftFail:
		return authres.ResultSoftFail
	case spf.Neutral:
		return authres.ResultNeutral
	case spf.TempError:
		return authres.ResultTempError
	case spf.PermError:
		return authres.ResultPermError
	default:
		return authres.ResultNone
	}
}

func dmarcSampleApplies(record *dmarc.Record, raw []byte, remoteIP net.IP) bool {
	if record == nil || record.Percent == nil || *record.Percent >= 100 {
		return true
	}
	if *record.Percent <= 0 {
		return false
	}
	h := sha256.New()
	_, _ = h.Write(raw)
	_, _ = h.Write(remoteIP)
	sum := h.Sum(nil)
	bucket := int(sum[0]) * 100 / 256
	return bucket < *record.Percent
}

func formatAuthenticationResults(identity string, results []authres.Result) string {
	value := strings.TrimSpace(authres.Format(cleanAuthValue(identity), results))
	if value == "" {
		return ""
	}
	if strings.HasPrefix(strings.ToLower(value), "authentication-results:") {
		if !strings.HasSuffix(value, "\r\n") {
			value += "\r\n"
		}
		return value
	}
	return "Authentication-Results: " + value + "\r\n"
}

func prependMessageHeader(raw []byte, header string) []byte {
	if header == "" {
		return raw
	}
	if !strings.HasSuffix(header, "\r\n") {
		header += "\r\n"
	}
	out := make([]byte, 0, len(header)+len(raw))
	out = append(out, header...)
	out = append(out, raw...)
	return out
}

func cleanAuthValue(value string) string {
	value = strings.NewReplacer("\r", "", "\n", "", ";", "_").Replace(strings.TrimSpace(value))
	if len(value) > 512 {
		value = value[:512]
	}
	return value
}

func cleanAuthReason(err error) string {
	if err == nil {
		return ""
	}
	value := cleanAuthValue(err.Error())
	if len(value) > 200 {
		value = value[:200]
	}
	return value
}

func logInboundAuthentication(action inboundAuthAction, remoteIP net.IP, envelopeFrom string) {
	if action == inboundAuthAccept {
		return
	}
	fingerprint := sha256.Sum256([]byte(envelopeFrom))
	log.Info("Mailbox inbound authentication action=%d remote=%s sender_hash=%s", action, remoteIP, hex.EncodeToString(fingerprint[:8]))
}
