package gateway

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestGetAssistantRepliesStableKeyAcrossRotation verifies the dedup key for a
// given reply does not change when the session JSONL is rotated/compacted and
// line indices shift — otherwise an already-delivered reply would be re-sent.
func TestGetAssistantRepliesStableKeyAcrossRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	msgA := `{"type":"message","timestamp":"2026-01-01T00:00:01Z","message":{"role":"assistant","stopReason":"stop","content":[{"type":"text","text":"first reply"}]}}`
	msgB := `{"type":"message","timestamp":"2026-01-01T00:00:02Z","message":{"role":"assistant","stopReason":"stop","content":[{"type":"text","text":"second reply"}]}}`
	userLine := `{"type":"message","timestamp":"2026-01-01T00:00:00Z","message":{"role":"user","stopReason":"","content":[{"type":"text","text":"hi"}]}}`

	// v1: the two assistant messages at line indices 0 and 1.
	writeSessionFile(t, path, msgA+"\n"+msgB+"\n")
	v1, err := getAssistantReplies(path, since)
	if err != nil {
		t.Fatalf("getAssistantReplies v1: %v", err)
	}
	if len(v1) != 2 {
		t.Fatalf("v1: expected 2 replies, got %d", len(v1))
	}

	// v2: same two messages, but a leading line shifts their indices to 1 and 2.
	writeSessionFile(t, path, userLine+"\n"+msgA+"\n"+msgB+"\n")
	v2, err := getAssistantReplies(path, since)
	if err != nil {
		t.Fatalf("getAssistantReplies v2: %v", err)
	}
	if len(v2) != 2 {
		t.Fatalf("v2: expected 2 replies, got %d", len(v2))
	}

	if v1[0].Key != v2[0].Key {
		t.Errorf("first reply key changed after rotation: %q -> %q", v1[0].Key, v2[0].Key)
	}
	if v1[1].Key != v2[1].Key {
		t.Errorf("second reply key changed after rotation: %q -> %q", v1[1].Key, v2[1].Key)
	}

	// Distinct replies must still produce distinct keys.
	if v1[0].Key == v1[1].Key {
		t.Errorf("distinct replies share a key: %q", v1[0].Key)
	}
}

func writeSessionFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
