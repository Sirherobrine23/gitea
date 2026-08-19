// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package mailbox

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/emersion/go-smtp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClassifySMTPError(t *testing.T) {
	assert.Nil(t, classifySMTPError(nil))

	// 5xx means the destination will never accept this message.
	permanent := classifySMTPError(&smtp.SMTPError{Code: 550, Message: "no such user"})
	require.NotNil(t, permanent)
	assert.True(t, permanent.permanent)
	assert.Equal(t, 550, permanent.code)

	// 4xx is a deferral and must be retried.
	temporary := classifySMTPError(&smtp.SMTPError{Code: 451, Message: "try later"})
	require.NotNil(t, temporary)
	assert.False(t, temporary.permanent)
	assert.Equal(t, 451, temporary.code)

	// A dropped connection is not a verdict on the message.
	network := classifySMTPError(errors.New("connection reset by peer"))
	require.NotNil(t, network)
	assert.False(t, network.permanent)
	assert.Equal(t, 0, network.code)
}

func TestClassifySMTPErrorUnwraps(t *testing.T) {
	// The delivery loop inspects errors with errors.As through wrapping.
	wrapped := temporaryFailure(0, "connect to mx.example.com: %w", &smtp.SMTPError{Code: 550, Message: "denied"})
	var delivery *deliveryError
	require.True(t, errors.As(error(wrapped), &delivery))
	assert.False(t, delivery.permanent)
}

func TestSortMailExchangers(t *testing.T) {
	hosts, err := sortMailExchangers("example.com", []*net.MX{
		{Host: "backup.example.com.", Pref: 20},
		{Host: "primary.example.com.", Pref: 10},
		{Host: "third.example.com.", Pref: 30},
	})
	require.NoError(t, err)
	// Lowest preference first, trailing dots stripped.
	assert.Equal(t, []string{"primary.example.com", "backup.example.com", "third.example.com"}, hosts)
}

func TestSortMailExchangersEqualPreferenceIsStable(t *testing.T) {
	hosts, err := sortMailExchangers("example.com", []*net.MX{
		{Host: "a.example.com.", Pref: 10},
		{Host: "b.example.com.", Pref: 10},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"a.example.com", "b.example.com"}, hosts)
}

func TestSortMailExchangersNullMX(t *testing.T) {
	// RFC 7505: "." means the domain accepts no mail, so never retry it.
	_, err := sortMailExchangers("example.com", []*net.MX{{Host: ".", Pref: 0}})
	require.Error(t, err)
	var delivery *deliveryError
	require.True(t, errors.As(err, &delivery))
	assert.True(t, delivery.permanent)

	_, err = sortMailExchangers("example.com", nil)
	assert.Error(t, err)
}

func TestGroupByDomain(t *testing.T) {
	grouped, err := groupByDomain([]string{
		"a@example.com", "b@example.com", "c@Example.ORG", " d@example.com ",
	})
	require.NoError(t, err)
	assert.Len(t, grouped, 2)
	assert.Len(t, grouped["example.com"], 3)
	assert.Len(t, grouped["example.org"], 1)

	_, err = groupByDomain([]string{"not-an-address"})
	assert.ErrorContains(t, err, "no domain")
}

func TestRetryDelayBacksOff(t *testing.T) {
	// Early attempts retry quickly, then widen and settle at an hour.
	assert.Equal(t, 5*time.Minute, retryDelay(1))
	assert.Equal(t, 15*time.Minute, retryDelay(2))
	assert.Equal(t, 30*time.Minute, retryDelay(3))
	assert.Equal(t, time.Hour, retryDelay(4))
	assert.Equal(t, time.Hour, retryDelay(50))

	previous := time.Duration(0)
	for attempt := 1; attempt <= 10; attempt++ {
		current := retryDelay(attempt)
		assert.GreaterOrEqual(t, current, previous, "backoff must never shrink")
		previous = current
	}
}
