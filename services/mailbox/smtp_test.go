// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package mailbox

import (
	"bufio"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSMTPPath(t *testing.T) {
	cases := []struct {
		in         string
		allowEmpty bool
		want       string
		wantErr    bool
	}{
		{in: "<user@example.com>", want: "user@example.com"},
		{in: "<user@example.com> SIZE=1234", want: "user@example.com"},
		{in: "<>", allowEmpty: true, want: ""},
		{in: "<>", wantErr: true},
		{in: "user@example.com", wantErr: true},
		{in: "<user@example.com", wantErr: true},
		// A display name is not an addr-spec and must be refused.
		{in: "<Bob <bob@example.com>>", wantErr: true},
		// A CR inside the path would let a peer inject a second command.
		{in: "<user\r\nDATA@example.com>", wantErr: true},
		// Trailing ESMTP parameters are discarded, so they cannot inject anything.
		{in: "<user@example.com>\r\nDATA", want: "user@example.com"},
	}
	for _, c := range cases {
		got, err := parseSMTPPath(c.in, c.allowEmpty)
		if c.wantErr {
			assert.Error(t, err, c.in)
			continue
		}
		require.NoError(t, err, c.in)
		assert.Equal(t, c.want, got, c.in)
	}
}

func TestTrimLineEnding(t *testing.T) {
	assert.Equal(t, "text", trimLineEnding("text\r\n"))
	assert.Equal(t, "text", trimLineEnding("text\n"))
	// Only one terminator is removed: extra CRs are body bytes that DKIM hashes.
	assert.Equal(t, "text\r", trimLineEnding("text\r\r\n"))
	assert.Equal(t, "text", trimLineEnding("text"))
}

func TestReadLimitedLine(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("first\r\nsecond\r\n"))
	line, err := readLimitedLine(r, 1024)
	require.NoError(t, err)
	assert.Equal(t, "first\r\n", line)

	line, err = readLimitedLine(r, 1024)
	require.NoError(t, err)
	assert.Equal(t, "second\r\n", line)

	_, err = readLimitedLine(r, 1024)
	assert.ErrorIs(t, err, io.EOF)
}

func TestReadLimitedLineRefusesOverlongLine(t *testing.T) {
	// A peer that never sends a newline must not be able to grow the buffer.
	_, err := readLimitedLine(bufio.NewReader(strings.NewReader(strings.Repeat("x", 200_000))), 1024)
	assert.ErrorIs(t, err, errLineTooLong)
}

func TestDecodePlainAuth(t *testing.T) {
	username, password, err := decodePlainAuth("AHVzZXIAcGFzcw==") // \0user\0pass
	require.NoError(t, err)
	assert.Equal(t, "user", username)
	assert.Equal(t, "pass", password)

	// An authorization identity different from the authentication identity is
	// impersonation and must be refused.
	_, _, err = decodePlainAuth("b3RoZXIAdXNlcgBwYXNz") // other\0user\0pass
	assert.Error(t, err)

	_, _, err = decodePlainAuth("bm90LWJhc2U2NC1zdHJ1Y3R1cmU=")
	assert.Error(t, err)
}

// unstuffDataLines mirrors the DATA reader so the transparency rules can be
// checked without a live connection.
func unstuffDataLines(t *testing.T, data string) string {
	t.Helper()
	r := bufio.NewReader(strings.NewReader(data))
	var out strings.Builder
	for {
		line, err := readLimitedLine(r, maxDataLineSize)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatal(err)
		}
		trimmed := trimLineEnding(line)
		if trimmed == "." {
			break
		}
		trimmed = strings.TrimPrefix(trimmed, ".")
		out.WriteString(trimmed)
		out.WriteString("\r\n")
		if errors.Is(err, io.EOF) {
			break
		}
	}
	return out.String()
}

func TestDataTransparency(t *testing.T) {
	// RFC 5321 4.5.2: one leading dot is stripped from every content line.
	assert.Equal(t, "body\r\n.stuffed\r\n", unstuffDataLines(t, "body\r\n..stuffed\r\n.\r\n"))
	assert.Equal(t, "leading\r\n", unstuffDataLines(t, ".leading\r\n.\r\n"))
	// A bare LF from a lax sender is normalized to CRLF before signing.
	assert.Equal(t, "a\r\nb\r\n", unstuffDataLines(t, "a\nb\n.\r\n"))
}
