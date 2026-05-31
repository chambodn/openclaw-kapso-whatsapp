package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// TestOpenClawReconnectsAfterDrop verifies the supervisor re-establishes the
// gateway connection after a drop and that requests succeed over the new socket.
func TestOpenClawReconnectsAfterDrop(t *testing.T) {
	upgrader := websocket.Upgrader{}
	var connCount int32
	connReady := make(chan int32, 8)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		n := atomic.AddInt32(&connCount, 1)

		// Auth handshake: challenge, read connect req, connect response.
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"payload":{"nonce":"abc"}}`))
		if _, _, err := conn.ReadMessage(); err != nil {
			_ = conn.Close()
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"res","id":"connect"}`))
		connReady <- n

		if n == 1 {
			// Drop the first connection to trigger a reconnect.
			_ = conn.Close()
			return
		}
		// Subsequent connections: echo a "res" for every request id.
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var f struct {
				ID string `json:"id"`
			}
			_ = json.Unmarshal(msg, &f)
			_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"res","id":"`+f.ID+`"}`))
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	oc := &OpenClaw{
		url:            wsURL,
		token:          "t",
		role:           "viewer",
		scopes:         []string{"viewer.read"},
		tracker:        newReplyTracker(),
		autoReconnect:  true,
		initialBackoff: time.Millisecond, // de-flake: don't pace on real time
		closing:        make(chan struct{}),
		supervisorDone: make(chan struct{}),
	}
	if err := oc.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() failed: %v", err)
	}
	defer func() { _ = oc.Close() }()

	if n := <-connReady; n != 1 {
		t.Fatalf("expected first connection, got %d", n)
	}

	// The supervisor must reconnect after the server drops connection 1.
	select {
	case n := <-connReady:
		if n != 2 {
			t.Fatalf("expected reconnect as connection 2, got %d", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for reconnect")
	}

	// A request now succeeds over the reconnected socket. sendRequest itself
	// waits for oc.cur to be swapped to the fresh session (bounded by ctx), so
	// no test-side spin is needed.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := oc.sendRequest(ctx, "ping", nil); err != nil {
		t.Fatalf("sendRequest after reconnect failed: %v", err)
	}
}

// TestOpenClawCloseStopsReconnect verifies Close stops the supervisor so it does
// not reconnect after shutdown.
func TestOpenClawCloseStopsReconnect(t *testing.T) {
	upgrader := websocket.Upgrader{}
	var connCount int32
	connReady := make(chan int32, 8)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		n := atomic.AddInt32(&connCount, 1)
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"payload":{"nonce":"abc"}}`))
		if _, _, err := conn.ReadMessage(); err != nil {
			_ = conn.Close()
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"res","id":"connect"}`))
		connReady <- n

		if n == 1 {
			// Drop connection 1 so the supervisor is genuinely mid-cycle
			// (reconnecting) when Close arrives — the realistic race.
			_ = conn.Close()
			return
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	oc := &OpenClaw{
		url:            wsURL,
		token:          "t",
		tracker:        newReplyTracker(),
		autoReconnect:  true,
		initialBackoff: time.Millisecond, // de-flake: don't pace on real time
		closing:        make(chan struct{}),
		supervisorDone: make(chan struct{}),
	}
	if err := oc.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() failed: %v", err)
	}

	// Wait for connection 1, then for the supervisor to reconnect as 2 — this
	// proves the supervisor is alive and reconnecting, not idle.
	if n := <-connReady; n != 1 {
		t.Fatalf("expected first connection, got %d", n)
	}
	select {
	case n := <-connReady:
		if n != 2 {
			t.Fatalf("expected reconnect as connection 2, got %d", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for reconnect")
	}

	// Close must stop the supervisor and join it (Close blocks on
	// supervisorDone), so no further connection can be opened afterward.
	_ = oc.Close()
	after := atomic.LoadInt32(&connCount)

	// Any post-Close reconnect would push another value onto connReady. With
	// the supervisor joined, none should arrive.
	select {
	case n := <-connReady:
		t.Fatalf("connection %d opened after Close: supervisor reconnected post-close", n)
	case <-time.After(100 * time.Millisecond):
	}
	if got := atomic.LoadInt32(&connCount); got != after {
		t.Fatalf("connection count grew after Close (%d -> %d): supervisor reconnected post-close", after, got)
	}
}
