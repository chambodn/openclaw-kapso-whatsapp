package gateway

import (
	"strings"
	"testing"
	"unicode/utf8"
)

// TestSplitMessageUTF8Boundary verifies a forced hard-cut never splits a
// multi-byte UTF-8 rune, so every chunk stays valid and no data is corrupted.
func TestSplitMessageUTF8Boundary(t *testing.T) {
	// "世界" is two 3-byte runes and contains no separators, so SplitMessage is
	// forced down to the hard-cut branch. maxLen=50 is not a multiple of 3, so
	// a naive byte cut would land in the middle of a rune.
	text := strings.Repeat("世界", 100) // 600 bytes, all 3-byte runes
	chunks := SplitMessage(text, 50)

	if len(chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(chunks))
	}
	for i, c := range chunks {
		if !utf8.ValidString(c) {
			t.Errorf("chunk %d is not valid UTF-8: %q", i, c)
		}
		if len(c) > 50 {
			t.Errorf("chunk %d is %d bytes, exceeds maxLen 50", i, len(c))
		}
	}
	// Rejoining must reproduce the original exactly (no rune split, no loss).
	if got := strings.Join(chunks, ""); got != text {
		t.Errorf("rejoined chunks differ from original (%d vs %d bytes)", len(got), len(text))
	}
}

// TestSplitMessageShortText returns the text unchanged when it fits.
func TestSplitMessageShortText(t *testing.T) {
	chunks := SplitMessage("héllo", 4096)
	if len(chunks) != 1 || chunks[0] != "héllo" {
		t.Fatalf("expected single unchanged chunk, got %v", chunks)
	}
}
