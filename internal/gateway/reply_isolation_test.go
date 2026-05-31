package gateway

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestGetSessionFileExactMatchNoSubstringLeak verifies that session resolution
// is exact and never resolves a sender onto another sender's transcript via a
// substring match — the cross-user leak vector. "bot-wa-1" is a substring of
// "bot-wa-15"; the old Contains() match could return either non-deterministically.
func TestGetSessionFileExactMatchNoSubstringLeak(t *testing.T) {
	sessionsJSON := filepath.Join(t.TempDir(), "sessions.json")
	// Verbatim per-sender keys — the format OpenClaw actually stores for an
	// explicitly-supplied session key.
	data := `{
		"bot-wa-1":  {"sessionFile": "/tmp/one.jsonl"},
		"bot-wa-15": {"sessionFile": "/tmp/fifteen.jsonl"}
	}`
	if err := os.WriteFile(sessionsJSON, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}

	if got, err := getSessionFile(sessionsJSON, "bot-wa-1"); err != nil || got != "/tmp/one.jsonl" {
		t.Fatalf(`getSessionFile("bot-wa-1") = %q, %v; want "/tmp/one.jsonl"`, got, err)
	}
	if got, err := getSessionFile(sessionsJSON, "bot-wa-15"); err != nil || got != "/tmp/fifteen.jsonl" {
		t.Fatalf(`getSessionFile("bot-wa-15") = %q, %v; want "/tmp/fifteen.jsonl"`, got, err)
	}
	// An absent key must error, not substring-match onto a present sender.
	if _, err := getSessionFile(sessionsJSON, "bot-wa-999"); err == nil {
		t.Fatal("expected error for absent key, got nil")
	}
}

// TestGetSessionFileLowercasesKey verifies resolution matches OpenClaw's
// lowercasing of explicit session keys.
func TestGetSessionFileLowercasesKey(t *testing.T) {
	sessionsJSON := filepath.Join(t.TempDir(), "sessions.json")
	if err := os.WriteFile(sessionsJSON, []byte(`{"bot-wa-15": {"sessionFile": "/tmp/f.jsonl"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := getSessionFile(sessionsJSON, "BOT-WA-15"); err != nil || got != "/tmp/f.jsonl" {
		t.Fatalf("expected case-insensitive exact match, got %q %v", got, err)
	}
}

// TestPollReplyFailsClosedOnMissingPerSenderSession verifies pollReply never
// substitutes another session's transcript when the per-sender session is
// missing: even though a claimable reply exists in the base session, polling a
// non-existent per-sender key returns the context error rather than leaking it.
func TestPollReplyFailsClosedOnMissingPerSenderSession(t *testing.T) {
	dir := t.TempDir()
	sessionsJSON := filepath.Join(dir, "sessions.json")
	baseFile := filepath.Join(dir, "base.jsonl")

	// Only the BASE session exists, and it has a ready assistant reply.
	if err := os.WriteFile(sessionsJSON, []byte(`{"bot": {"sessionFile": "`+baseFile+`"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	ts := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	reply := `{"type":"message","timestamp":"` + ts + `","message":{"role":"assistant","stopReason":"stop","content":[{"type":"text","text":"base reply"}]}}`
	if err := os.WriteFile(baseFile, []byte(reply+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	oc := &OpenClaw{
		sessionsJSON: sessionsJSON,
		sessionKey:   "bot",
		tracker:      newReplyTracker(),
		pollInterval: 2 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(40 * time.Millisecond)
		cancel()
	}()

	text, err := oc.pollReply(ctx, "bot-wa-15")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got text=%q err=%v", text, err)
	}
	if text != "" {
		t.Fatalf("fail-closed violated: leaked reply %q from another session", text)
	}
}
