package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/Enriquefft/openclaw-kapso-whatsapp/internal/config"
	"github.com/Enriquefft/openclaw-kapso-whatsapp/internal/safe"
	"github.com/gorilla/websocket"
)

// OpenClaw protocol types.

type requestFrame struct {
	Type   string      `json:"type"`
	ID     string      `json:"id"`
	Method string      `json:"method"`
	Params interface{} `json:"params,omitempty"`
}

type responseFrame struct {
	Type   string          `json:"type"`
	ID     string          `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  json.RawMessage `json:"error,omitempty"`
}

type connectParams struct {
	MinProtocol int         `json:"minProtocol"`
	MaxProtocol int         `json:"maxProtocol"`
	Client      clientInfo  `json:"client"`
	Auth        authInfo    `json:"auth"`
	Device      *DeviceInfo `json:"device,omitempty"`
	Role        string      `json:"role"`
	Scopes      []string    `json:"scopes"`
}

// DeviceInfo identifies this device to the gateway via a signed challenge.
type DeviceInfo struct {
	ID        string `json:"id"`
	PublicKey string `json:"publicKey"`
	Signature string `json:"signature"`
	SignedAt  int64  `json:"signedAt"`
	Nonce     string `json:"nonce"`
}

// Signer provides device identity for the gateway connect handshake.
type Signer interface {
	DeviceID() string
	PublicKeyBase64() string
	Sign(data []byte) []byte
}

type clientInfo struct {
	ID          string `json:"id"`
	DisplayName string `json:"displayName"`
	Version     string `json:"version"`
	Platform    string `json:"platform"`
	Mode        string `json:"mode"`
}

type authInfo struct {
	Token string `json:"token"`
}

type chatSendParams struct {
	SessionKey     string `json:"sessionKey"`
	Message        string `json:"message"`
	IdempotencyKey string `json:"idempotencyKey"`
}

// Version is the bridge version sent in the connect handshake.
// Overridden at build time via -ldflags.
var Version = "dev"

// maxClaimed caps the replyTracker map size. Entries older than this many
// replies are irrelevant for dedup — the polling window is 10 min.
const maxClaimed = 1000

// replyTracker prevents concurrent relay goroutines from claiming the same
// assistant reply in the session JSONL.
type replyTracker struct {
	mu      sync.Mutex
	claimed map[string]bool
}

func newReplyTracker() *replyTracker {
	return &replyTracker{claimed: make(map[string]bool)}
}

func (rt *replyTracker) claim(key string) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.claimed[key] {
		return false
	}
	if len(rt.claimed) >= maxClaimed {
		rt.claimed = make(map[string]bool)
	}
	rt.claimed[key] = true
	return true
}

type assistantReply struct {
	Key  string
	Text string
}

// connSession holds the state for one live gateway connection. A fresh one is
// created on every (re)connect; readLoop, pingLoop, and sendRequest each bind to
// a specific session, so a reconnect can never tangle two generations (e.g.
// route a response to the wrong connection's pending map, or close the wrong
// done channel).
type connSession struct {
	conn    *websocket.Conn
	wmu     sync.Mutex // serialises WriteMessage calls + seq on this conn
	seq     int
	pendMu  sync.Mutex
	pending map[string]chan responseFrame // request id -> response channel
	done    chan struct{}                 // closed once readLoop exits
}

func (cs *connSession) nextID() string {
	cs.wmu.Lock()
	cs.seq++
	id := cs.seq
	cs.wmu.Unlock()
	return fmt.Sprintf("kapso-%d", id)
}

// OpenClaw implements Gateway for the OpenClaw agent runtime.
type OpenClaw struct {
	url          string
	token        string
	signer       Signer
	sessionsJSON string
	sessionKey   string
	role         string
	scopes       []string
	tracker      *replyTracker
	pollInterval time.Duration // session-poll cadence; injectable for tests

	autoReconnect bool // reconnect on drop; true via the production constructors

	mu        sync.Mutex    // guards cur and closing
	cur       *connSession  // current connection; nil before connect / mid-reconnect
	closing   chan struct{} // closed by Close to stop the supervisor
	closeOnce sync.Once
}

// NewOpenClaw creates an OpenClaw gateway from config.
func NewOpenClaw(cfg config.GatewayConfig) *OpenClaw {
	return &OpenClaw{
		url:           cfg.URL,
		token:         cfg.Token,
		sessionsJSON:  cfg.SessionsJSON,
		sessionKey:    cfg.SessionKey,
		role:          cfg.Role,
		scopes:        cfg.Scopes,
		tracker:       newReplyTracker(),
		pollInterval:  3 * time.Second,
		autoReconnect: true,
		closing:       make(chan struct{}),
	}
}

// NewOpenClawWithSigner creates an OpenClaw gateway with a device identity signer.
func NewOpenClawWithSigner(cfg config.GatewayConfig, signer Signer) *OpenClaw {
	oc := NewOpenClaw(cfg)
	oc.signer = signer
	return oc
}

// Connect establishes the first connection and, in production, starts the
// supervisor that reconnects on drops.
func (oc *OpenClaw) Connect(ctx context.Context) error {
	if err := oc.connectOnce(ctx); err != nil {
		return err
	}
	if oc.autoReconnect {
		oc.mu.Lock()
		if oc.closing == nil {
			oc.closing = make(chan struct{})
		}
		oc.mu.Unlock()
		go oc.supervise(ctx)
	}
	return nil
}

// connectOnce dials the gateway and completes the challenge-response auth
// handshake, then installs the resulting connSession and starts its read/ping
// loops. It is called for the initial connect and for every reconnect.
func (oc *OpenClaw) connectOnce(ctx context.Context) error {
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.DialContext(ctx, oc.url, nil)
	if err != nil {
		return fmt.Errorf("connect to gateway: %w", err)
	}
	cs := &connSession{
		conn:    conn,
		pending: make(map[string]chan responseFrame),
		done:    make(chan struct{}),
	}

	// Read the challenge from the gateway.
	_ = conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("read challenge: %w", err)
	}

	log.Printf("received challenge from gateway (%d bytes)", len(msg))

	// Parse challenge to extract nonce for device signing.
	var challenge struct {
		Payload struct {
			Nonce string `json:"nonce"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(msg, &challenge); err != nil {
		_ = conn.Close()
		return fmt.Errorf("parse challenge frame: %w", err)
	}

	clientID := "gateway-client"
	clientMode := "backend"
	role := oc.role
	scopes := oc.scopes
	platform := runtime.GOOS

	// Build device identity if a signer is configured.
	var deviceInfo *DeviceInfo
	if oc.signer == nil {
		log.Printf("warning: connecting without device identity — gateway may reject scoped operations")
	} else {
		nonce := challenge.Payload.Nonce
		if nonce == "" {
			_ = conn.Close()
			return fmt.Errorf("gateway challenge missing nonce")
		}
		signedAt := time.Now().UnixMilli()
		payload := buildDeviceAuthPayloadV3(oc.signer.DeviceID(), clientID, clientMode, role, oc.token, scopes, signedAt, nonce, platform, "")
		sig := oc.signer.Sign([]byte(payload))
		deviceInfo = &DeviceInfo{
			ID:        oc.signer.DeviceID(),
			PublicKey: oc.signer.PublicKeyBase64(),
			Signature: base64.RawURLEncoding.EncodeToString(sig),
			SignedAt:  signedAt,
			Nonce:     nonce,
		}
	}

	// Send connect request.
	connectReq := requestFrame{
		Type:   "req",
		ID:     cs.nextID(),
		Method: "connect",
		Params: connectParams{
			// Advertise a protocol range. The gateway accepts the connection
			// when this range includes its PROTOCOL_VERSION. OpenClaw
			// 2026.5.27 requires protocol 4; older gateways speak 3. The
			// device-auth payload (v3, see buildDeviceAuthPayloadV3) is
			// unchanged across both, so the [3,4] range is backward compatible.
			MinProtocol: 3,
			MaxProtocol: 4,
			Client: clientInfo{
				ID:          clientID,
				DisplayName: "Kapso WhatsApp Bridge",
				Version:     Version,
				Platform:    platform,
				Mode:        clientMode,
			},
			Auth: authInfo{
				Token: oc.token,
			},
			Device: deviceInfo,
			Role:   role,
			Scopes: scopes,
		},
	}

	data, err := json.Marshal(connectReq)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("marshal connect request: %w", err)
	}

	log.Printf("sending connect request")

	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		_ = conn.Close()
		return fmt.Errorf("send connect: %w", err)
	}

	// Wait for response.
	_ = conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	_, msg, err = conn.ReadMessage()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("read connect response: %w", err)
	}

	log.Printf("received connect response (%d bytes)", len(msg))

	var resp responseFrame
	if err := json.Unmarshal(msg, &resp); err != nil {
		_ = conn.Close()
		return fmt.Errorf("parse connect response: %w", err)
	}

	if resp.Error != nil {
		_ = conn.Close()
		return fmt.Errorf("connect rejected: %s", string(resp.Error))
	}

	// Keepalive: bound reads so a half-open connection cannot hang readLoop
	// (and every pending caller) indefinitely. The deadline is extended on any
	// received frame and on pong replies; a ping is sent each pingPeriod.
	//
	// Agent replies are delivered via JSONL polling (see pollReply), NOT over
	// this socket, so in steady state the connection is data-idle and liveness
	// depends on the gateway answering our control pings with pongs (standard
	// RFC 6455 behaviour). If a gateway never ponged, the connection would be
	// torn down every pongWait; the supervisor then reconnects with backoff.
	_ = conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	oc.mu.Lock()
	oc.cur = cs
	oc.mu.Unlock()
	go oc.pingLoop(cs)
	go oc.readLoop(cs)

	log.Printf("authenticated with gateway at %s", oc.url)
	return nil
}

// supervise reconnects the gateway whenever the live connection drops, until
// the daemon context is cancelled or Close is called. It runs only when
// autoReconnect is set (the production constructors). Each iteration waits for
// the current session's readLoop to exit, then reconnects with backoff.
func (oc *OpenClaw) supervise(ctx context.Context) {
	defer safe.Recover("openclaw supervise")
	for {
		oc.mu.Lock()
		cs := oc.cur
		oc.mu.Unlock()
		if cs == nil {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-oc.closing:
			return
		case <-cs.done:
		}

		// Connection dropped. Stop if we're shutting down; otherwise reconnect.
		select {
		case <-ctx.Done():
			return
		case <-oc.closing:
			return
		default:
		}

		log.Printf("openclaw: gateway connection lost, reconnecting...")
		if !oc.reconnect(ctx) {
			return
		}
		log.Printf("openclaw: reconnected to gateway")
	}
}

// reconnect retries connectOnce with exponential backoff until it succeeds or
// the daemon is shutting down. Returns false if it stopped without connecting.
func (oc *OpenClaw) reconnect(ctx context.Context) bool {
	const maxBackoff = 30 * time.Second
	backoff := time.Second
	for {
		if err := oc.connectOnce(ctx); err == nil {
			return true
		} else {
			log.Printf("openclaw: reconnect attempt failed: %v (retrying in %s)", err, backoff)
		}
		select {
		case <-ctx.Done():
			return false
		case <-oc.closing:
			return false
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// WebSocket keepalive timings. A ping is sent every pingPeriod; the read side
// must see a frame (data or pong) within pongWait or the read fails, freeing
// any callers blocked on a half-open connection. pingPeriod < pongWait.
const (
	pongWait   = 90 * time.Second
	pingPeriod = 54 * time.Second
)

// pingLoop sends a periodic WebSocket ping so an idle-but-healthy connection
// stays alive and a dead one is detected within pongWait. WriteControl is safe
// to call concurrently with the writes in sendRequest. It exits when the
// session's done channel is closed (i.e. readLoop has returned).
func (oc *OpenClaw) pingLoop(cs *connSession) {
	defer safe.Recover("openclaw pingLoop")
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-cs.done:
			return
		case <-ticker.C:
			if err := cs.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(10*time.Second)); err != nil {
				return
			}
		}
	}
}

// readLoop reads incoming frames for one connection and routes "res" frames to
// that session's pending callers. It is the sole reader of the session's conn;
// on exit it fails all pending callers and closes the session's done channel,
// which is what the supervisor waits on to trigger a reconnect.
func (oc *OpenClaw) readLoop(cs *connSession) {
	defer safe.Recover("openclaw readLoop")
	defer func() {
		// Signal all pending sendRequest callers that the connection is gone.
		cs.pendMu.Lock()
		for id, ch := range cs.pending {
			close(ch)
			delete(cs.pending, id)
		}
		cs.pendMu.Unlock()
		close(cs.done)
	}()

	for {
		_, msg, err := cs.conn.ReadMessage()
		if err != nil {
			return
		}
		// Any traffic proves the connection is alive — extend the deadline.
		_ = cs.conn.SetReadDeadline(time.Now().Add(pongWait))

		var frame responseFrame
		if err := json.Unmarshal(msg, &frame); err != nil {
			log.Printf("openclaw: ignoring unparseable frame (%d bytes)", len(msg))
			continue
		}

		// Route responses to waiting callers by request ID.
		if frame.Type == "res" && frame.ID != "" {
			cs.pendMu.Lock()
			if ch, ok := cs.pending[frame.ID]; ok {
				ch <- frame
				delete(cs.pending, frame.ID)
			}
			cs.pendMu.Unlock()
			continue
		}

		log.Printf("gateway event: type=%s method=%s (%d bytes)", frame.Type, frame.Method, len(msg))
	}
}

// sendRequest sends a request frame and waits for the matching response. It
// binds to the current connSession for its whole lifetime, so an in-flight
// request is unaffected by a concurrent reconnect (it waits on its own session's
// done channel, not a swapped field).
func (oc *OpenClaw) sendRequest(ctx context.Context, method string, params interface{}) (responseFrame, error) {
	oc.mu.Lock()
	cs := oc.cur
	oc.mu.Unlock()
	if cs == nil {
		return responseFrame{}, fmt.Errorf("not connected to gateway")
	}

	id := cs.nextID()
	req := requestFrame{
		Type:   "req",
		ID:     id,
		Method: method,
		Params: params,
	}

	data, err := json.Marshal(req)
	if err != nil {
		return responseFrame{}, fmt.Errorf("marshal %s request: %w", method, err)
	}

	// Register response channel before sending so readLoop can't race us.
	ch := make(chan responseFrame, 1)
	cs.pendMu.Lock()
	cs.pending[id] = ch
	cs.pendMu.Unlock()

	// Serialise writes on this conn (gorilla requires it); WriteControl pings
	// from pingLoop remain safe concurrently.
	cs.wmu.Lock()
	err = cs.conn.WriteMessage(websocket.TextMessage, data)
	cs.wmu.Unlock()
	if err != nil {
		cs.pendMu.Lock()
		delete(cs.pending, id)
		cs.pendMu.Unlock()
		return responseFrame{}, fmt.Errorf("send %s: %w", method, err)
	}

	// Wait for readLoop to deliver the response.
	select {
	case resp, ok := <-ch:
		if !ok {
			return responseFrame{}, fmt.Errorf("connection closed while waiting for %s response", method)
		}
		return resp, nil
	case <-ctx.Done():
		cs.pendMu.Lock()
		delete(cs.pending, id)
		cs.pendMu.Unlock()
		return responseFrame{}, ctx.Err()
	case <-cs.done:
		return responseFrame{}, fmt.Errorf("connection closed while waiting for %s response", method)
	}
}

// SendAndReceive sends a message to the OpenClaw gateway and polls the
// session JSONL until the agent produces a reply.
func (oc *OpenClaw) SendAndReceive(ctx context.Context, req *Request) (string, error) {
	// Format message with sender metadata — OpenClaw convention.
	taggedText := fmt.Sprintf("From: %s (%s) [role: %s]\n%s",
		req.From, req.FromName, req.Role, req.Text)

	sessionKey := req.SessionKey
	if sessionKey == "" {
		sessionKey = oc.sessionKey
	}

	// Send message and wait for the gateway's acknowledgement.
	resp, err := oc.sendRequest(ctx, "chat.send", chatSendParams{
		SessionKey:     sessionKey,
		Message:        taggedText,
		IdempotencyKey: req.IdempotencyKey,
	})
	if err != nil {
		return "", fmt.Errorf("chat.send: %w", err)
	}
	if resp.Error != nil {
		return "", fmt.Errorf("chat.send rejected: %s", string(resp.Error))
	}

	// Poll session JSONL for the agent's reply.
	return oc.pollReply(ctx, sessionKey)
}

// pollReply polls the given session's JSONL transcript until an unclaimed
// assistant reply appears. It resolves ONLY that session key and never falls
// back to another session: OpenClaw creates the per-sender session eagerly when
// it handles chat.send (the sessions.json entry and sessionFile are written
// before the agent replies), so the transcript appears within a poll tick.
// Failing closed preserves cross-user isolation — a fallback to a shared base
// session could deliver one user's reply to another.
func (oc *OpenClaw) pollReply(ctx context.Context, sessionKey string) (string, error) {
	since := time.Now().UTC()
	deadline := time.Now().Add(10 * time.Minute)
	interval := oc.pollInterval
	if interval <= 0 {
		interval = 3 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	loggedMissing := false

	for {
		if time.Now().After(deadline) {
			return "", fmt.Errorf("timeout waiting for agent reply (session %s)", sessionKey)
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
		}

		sessionFile, err := getSessionFile(oc.sessionsJSON, sessionKey)
		if err != nil {
			// Fail closed — keep polling this session only; never substitute
			// another session's transcript. Log only the first occurrence so a
			// stuck request produces one line, not one per tick.
			if !loggedMissing {
				log.Printf("openclaw: %v", err)
				loggedMissing = true
			}
			continue
		}

		replies, err := getAssistantReplies(sessionFile, since)
		if err != nil {
			log.Printf("openclaw: error reading session: %v", err)
			continue
		}

		for _, reply := range replies {
			if oc.tracker.claim(reply.Key) {
				return reply.Text, nil
			}
		}
	}
}

// Close stops the supervisor and closes the current connection, waiting for its
// readLoop to exit.
func (oc *OpenClaw) Close() error {
	// Stop the supervisor first so it does not reconnect while we tear down.
	oc.mu.Lock()
	if oc.closing != nil {
		oc.closeOnce.Do(func() { close(oc.closing) })
	}
	cs := oc.cur
	oc.cur = nil
	oc.mu.Unlock()

	if cs == nil {
		return nil
	}
	err := cs.conn.Close()
	// Wait for readLoop to finish cleanup. The wait is bounded because
	// conn.Close() causes ReadMessage() to return an error immediately.
	<-cs.done
	return err
}

func buildDeviceAuthPayloadV3(deviceID, clientID, clientMode, role, token string, scopes []string, signedAtMs int64, nonce, platform, deviceFamily string) string {
	return strings.Join([]string{
		"v3",
		deviceID,
		clientID,
		clientMode,
		role,
		strings.Join(scopes, ","),
		fmt.Sprintf("%d", signedAtMs),
		token,
		nonce,
		normalizeMetadata(platform),
		normalizeMetadata(deviceFamily),
	}, "|")
}

func normalizeMetadata(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

// getSessionFile reads sessions.json and returns the path to the active session
// JSONL file for the given session key, resolved by EXACT match.
//
// OpenClaw stores an explicitly-supplied session key verbatim (lowercased) as
// the store key, so an exact lookup is correct. It must NOT substring-match:
// with per-sender keys like "<base>-wa-<digits>", one sender's key is a
// substring of another's ("...-wa-1" is contained in "...-wa-15"), so a loose
// match could resolve a sender onto another sender's transcript — a cross-user
// reply leak.
func getSessionFile(sessionsJSON, sessionKey string) (string, error) {
	data, err := os.ReadFile(sessionsJSON)
	if err != nil {
		return "", fmt.Errorf("read sessions.json: %w", err)
	}

	var sessions map[string]struct {
		SessionFile string `json:"sessionFile"`
	}
	if err := json.Unmarshal(data, &sessions); err != nil {
		return "", fmt.Errorf("parse sessions.json: %w", err)
	}

	key := strings.ToLower(strings.TrimSpace(sessionKey))
	if s, ok := sessions[key]; ok && s.SessionFile != "" {
		return s.SessionFile, nil
	}
	// Backward-compatible exact fallback for any session stored under the
	// canonical "agent:KEY:KEY" form (derived, non-explicit sessions). Still an
	// exact lookup — no substring matching.
	if s, ok := sessions["agent:"+key+":"+key]; ok && s.SessionFile != "" {
		return s.SessionFile, nil
	}

	return "", fmt.Errorf("no session file found for key %q in %s", sessionKey, sessionsJSON)
}

// getAssistantReplies scans the session JSONL for all assistant messages with
// stopReason=stop that were recorded after `since`.
func getAssistantReplies(sessionFile string, since time.Time) ([]assistantReply, error) {
	data, err := os.ReadFile(sessionFile)
	if err != nil {
		return nil, err
	}

	var replies []assistantReply
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var entry struct {
			Type      string    `json:"type"`
			Timestamp time.Time `json:"timestamp"`
			Message   struct {
				Role       string `json:"role"`
				StopReason string `json:"stopReason"`
				Content    []struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"content"`
			} `json:"message"`
		}

		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}

		if entry.Type != "message" || entry.Timestamp.Before(since) {
			continue
		}
		if entry.Message.Role != "assistant" || entry.Message.StopReason != "stop" {
			continue
		}

		var texts []string
		for _, block := range entry.Message.Content {
			if block.Type == "text" && block.Text != "" {
				texts = append(texts, block.Text)
			}
		}
		if len(texts) > 0 {
			text := strings.Join(texts, "\n")
			replies = append(replies, assistantReply{
				Key:  stableReplyKey(sessionFile, entry.Timestamp, text),
				Text: text,
			})
		}
	}

	return replies, nil
}

// stableReplyKey derives a dedup key from the reply's session, timestamp, and
// content hash rather than its line position. Line indices shift when the
// session JSONL is rotated or compacted, which would make an already-delivered
// reply look new and re-send it; timestamp+content is stable across rotation.
func stableReplyKey(sessionFile string, ts time.Time, text string) string {
	sum := sha256.Sum256([]byte(text))
	return fmt.Sprintf("%s:%s:%x", sessionFile, ts.UTC().Format(time.RFC3339Nano), sum[:8])
}
