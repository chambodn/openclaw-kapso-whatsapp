// Package phone normalizes WhatsApp phone numbers to a canonical, digits-only
// key. The Meta/WhatsApp webhook sends `from` without a leading +, while config
// entries are commonly written with one; normalizing both to digits only lets
// "+1 (234) 567-890", "1234567890", and "+1234567890" all match.
package phone

import "strings"

// Normalize strips every non-digit rune (including a leading +) from s, so the
// same number written with or without punctuation or a + prefix yields the same
// key. An empty input returns an empty string.
func Normalize(s string) string {
	if s == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}
