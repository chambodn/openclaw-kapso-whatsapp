package poller

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Enriquefft/openclaw-kapso-whatsapp/internal/delivery"
	"github.com/Enriquefft/openclaw-kapso-whatsapp/internal/kapso"
	"github.com/Enriquefft/openclaw-kapso-whatsapp/internal/transcribe"
)

// Poller implements delivery.Source by polling the Kapso list-messages API.
type Poller struct {
	Client       *kapso.Client
	Interval     time.Duration
	StateDir     string
	StateFile    string
	Transcriber  transcribe.Transcriber // nil = transcription disabled
	MaxAudioSize int64
}

// Run polls the Kapso API on a ticker and emits events for each new inbound
// message. It returns when ctx is cancelled.
func (p *Poller) Run(ctx context.Context, out chan<- delivery.Event) error {
	if err := os.MkdirAll(p.StateDir, 0o700); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}

	lastPoll := loadState(p.StateFile)
	if lastPoll.IsZero() {
		lastPoll = time.Now().UTC()
		saveState(p.StateFile, lastPoll)
		log.Printf("first run, starting from %s", lastPoll.Format(time.RFC3339))
	}

	// Poll immediately, then on interval.
	p.poll(&lastPoll, out)

	ticker := time.NewTicker(p.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			p.poll(&lastPoll, out)
		}
	}
}

// maxPollPages bounds how many pages a single poll cycle will drain. The Kapso
// list API returns at most Limit messages per page; following the `after`
// cursor lets one cycle consume a full backlog instead of silently dropping
// everything past the first page. The cap guards against a misbehaving API
// (e.g. a non-terminating cursor) rather than the normal workload. Because the
// API walks forward in time, advancing the cursor to the newest message we did
// fetch is safe when the cap is hit — newer messages are picked up next cycle
// by the `since` filter.
const maxPollPages = 100

func (p *Poller) poll(lastPoll *time.Time, out chan<- delivery.Event) {
	since := lastPoll.Format(time.RFC3339)

	var newest time.Time
	forwarded := 0
	after := ""

	for page := 0; page < maxPollPages; page++ {
		resp, err := p.Client.ListMessages(kapso.ListMessagesParams{
			Direction: "inbound",
			Since:     since,
			Limit:     100,
			After:     after,
		})
		if err != nil {
			// Do not advance the cursor on a mid-drain error; retry next cycle.
			log.Printf("poll error: %v", err)
			return
		}

		for _, msg := range resp.Data {
			// Track timestamp for ALL messages so the cursor advances past
			// unsupported types (stickers, contacts, etc.) and they are not
			// re-fetched on the next poll cycle.
			msgTime := parseTimestamp(msg.Timestamp)
			if !msgTime.IsZero() && msgTime.After(newest) {
				newest = msgTime
			}

			text, ok := delivery.ExtractText(msg.Message, p.Client, p.Transcriber, p.MaxAudioSize)
			if !ok {
				continue
			}

			name := ""
			if msg.Kapso != nil {
				name = msg.Kapso.ContactName
			}

			out <- delivery.Event{
				ID:   msg.ID,
				From: msg.From,
				Name: name,
				Text: text,
			}
			forwarded++
		}

		next := ""
		if resp.Paging != nil {
			next = resp.Paging.Cursors.After
		}
		// Stop when there is no next page, the page was empty, or the cursor
		// failed to advance (defensive against a stuck cursor that would loop).
		if next == "" || len(resp.Data) == 0 || next == after {
			break
		}
		after = next

		if page == maxPollPages-1 {
			log.Printf("WARN: poll hit max page limit (%d); newer messages will be fetched next cycle", maxPollPages)
		}
	}

	if forwarded > 0 {
		log.Printf("forwarded %d message(s)", forwarded)
	}

	if !newest.IsZero() {
		*lastPoll = newest.Add(time.Second)
		saveState(p.StateFile, *lastPoll)
	}
}

func parseTimestamp(s string) time.Time {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t
	}
	if n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64); err == nil {
		return time.Unix(n, 0).UTC()
	}
	return time.Time{}
}

func loadState(path string) time.Time {
	data, err := os.ReadFile(path)
	if err != nil {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, strings.TrimSpace(string(data)))
	if err != nil {
		return time.Time{}
	}
	return t
}

func saveState(path string, t time.Time) {
	if err := os.WriteFile(path, []byte(t.Format(time.RFC3339)), 0o600); err != nil {
		log.Printf("WARN: failed to save poll state: %v", err)
	}
}
