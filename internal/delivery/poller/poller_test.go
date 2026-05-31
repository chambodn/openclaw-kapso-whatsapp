package poller

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Enriquefft/openclaw-kapso-whatsapp/internal/delivery"
	"github.com/Enriquefft/openclaw-kapso-whatsapp/internal/kapso"
)

// TestPollFollowsPaginationCursor verifies the poller drains every page of a
// poll window by following the `after` cursor, rather than dropping everything
// past the first page.
func TestPollFollowsPaginationCursor(t *testing.T) {
	var reqCount int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&reqCount, 1)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("after") {
		case "":
			// Page 1: two messages and a cursor to page 2.
			_, _ = fmt.Fprint(w, `{"data":[
				{"id":"m1","type":"text","from":"111","timestamp":"100","text":{"body":"one"}},
				{"id":"m2","type":"text","from":"222","timestamp":"200","text":{"body":"two"}}
			],"paging":{"cursors":{"after":"CURSOR1"}}}`)
		case "CURSOR1":
			// Page 2: one message, no further cursor.
			_, _ = fmt.Fprint(w, `{"data":[
				{"id":"m3","type":"text","from":"333","timestamp":"300","text":{"body":"three"}}
			]}`)
		default:
			t.Errorf("unexpected after cursor %q", r.URL.Query().Get("after"))
			_, _ = fmt.Fprint(w, `{"data":[]}`)
		}
	}))
	defer srv.Close()

	client := &kapso.Client{
		APIKey:        "k",
		PhoneNumberID: "pid",
		BaseURL:       srv.URL,
		HTTPClient:    srv.Client(),
	}
	p := &Poller{
		Client:    client,
		StateDir:  t.TempDir(),
		StateFile: filepath.Join(t.TempDir(), "last-poll"),
	}

	out := make(chan delivery.Event, 10)
	lastPoll := time.Unix(0, 0).UTC()
	p.poll(context.Background(), &lastPoll, out)
	close(out)

	var ids []string
	for evt := range out {
		ids = append(ids, evt.ID)
	}

	if len(ids) != 3 {
		t.Fatalf("expected 3 events across 2 pages, got %d: %v", len(ids), ids)
	}
	if n := atomic.LoadInt32(&reqCount); n != 2 {
		t.Fatalf("expected 2 API requests (one per page), got %d", n)
	}

	// Cursor advances to the newest message timestamp (+1s).
	wantLast := time.Unix(300, 0).UTC().Add(time.Second)
	if !lastPoll.Equal(wantLast) {
		t.Errorf("lastPoll = %v, want %v", lastPoll, wantLast)
	}
}

// TestPollSinglePageNoCursor verifies a single-page window terminates after one
// request when no pagination cursor is returned.
func TestPollSinglePageNoCursor(t *testing.T) {
	var reqCount int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&reqCount, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"data":[
			{"id":"only","type":"text","from":"111","timestamp":"100","text":{"body":"hi"}}
		]}`)
	}))
	defer srv.Close()

	client := &kapso.Client{APIKey: "k", PhoneNumberID: "pid", BaseURL: srv.URL, HTTPClient: srv.Client()}
	p := &Poller{Client: client, StateDir: t.TempDir(), StateFile: filepath.Join(t.TempDir(), "last-poll")}

	out := make(chan delivery.Event, 4)
	lastPoll := time.Unix(0, 0).UTC()
	p.poll(context.Background(), &lastPoll, out)
	close(out)

	var ids []string
	for evt := range out {
		ids = append(ids, evt.ID)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 event, got %d: %v", len(ids), ids)
	}
	if n := atomic.LoadInt32(&reqCount); n != 1 {
		t.Fatalf("expected exactly 1 API request, got %d", n)
	}
}
