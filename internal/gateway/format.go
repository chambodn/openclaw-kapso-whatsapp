package gateway

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

// Compiled regexes for mdToWhatsApp compiled once at startup.
var (
	reBold       = regexp.MustCompile(`\*\*(.+?)\*\*`)
	reItalic     = regexp.MustCompile(`\*(.+?)\*`)
	reStrike     = regexp.MustCompile(`~~(.+?)~~`)
	reHeading    = regexp.MustCompile("(?m)^#{1,3} +(.+)$")
	reBlockquote = regexp.MustCompile("(?m)^> ?")
)

// MdToWhatsApp converts Markdown formatting to WhatsApp-compatible formatting.
func MdToWhatsApp(text string) string {
	const boldMarker = "\x01"

	result := reBold.ReplaceAllString(text, boldMarker+"$1"+boldMarker)
	result = reItalic.ReplaceAllString(result, "_$1_")
	result = strings.ReplaceAll(result, boldMarker, "*")
	result = reStrike.ReplaceAllString(result, "~$1~")
	result = reHeading.ReplaceAllString(result, "*$1*")
	result = reBlockquote.ReplaceAllString(result, "")

	return result
}

// SplitMessage splits text into chunks of at most maxLen bytes.
func SplitMessage(text string, maxLen int) []string {
	if len(text) <= maxLen {
		return []string{text}
	}

	minSplit := maxLen / 4
	var chunks []string

	for len(text) > maxLen {
		chunk := text[:maxLen]

		if i := strings.LastIndex(chunk, "\n\n"); i >= minSplit {
			chunks = append(chunks, strings.TrimSpace(text[:i]))
			text = strings.TrimSpace(text[i:])
			continue
		}

		if i := strings.LastIndex(chunk, "\n"); i >= minSplit {
			chunks = append(chunks, strings.TrimSpace(text[:i]))
			text = strings.TrimSpace(text[i:])
			continue
		}

		splitPos := -1
		for _, sep := range []string{". ", "? ", "! "} {
			if i := strings.LastIndex(chunk, sep); i >= minSplit {
				pos := i + 1
				if pos > splitPos {
					splitPos = pos
				}
			}
		}
		if splitPos >= 0 {
			chunks = append(chunks, strings.TrimSpace(text[:splitPos]))
			text = strings.TrimSpace(text[splitPos:])
			continue
		}

		if i := strings.LastIndex(chunk, " "); i >= minSplit {
			chunks = append(chunks, strings.TrimSpace(text[:i]))
			text = strings.TrimSpace(text[i:])
			continue
		}

		// No separator found within range — hard-cut at maxLen, but back off to
		// the nearest UTF-8 rune boundary so a multi-byte rune is never split.
		cut := maxLen
		for cut > 0 && !utf8.RuneStart(text[cut]) {
			cut--
		}
		chunks = append(chunks, strings.TrimSpace(text[:cut]))
		text = strings.TrimSpace(text[cut:])
	}

	if text != "" {
		chunks = append(chunks, strings.TrimSpace(text))
	}

	return chunks
}
