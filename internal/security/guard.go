package security

import (
	"sync"
	"time"

	"github.com/Enriquefft/openclaw-kapso-whatsapp/internal/config"
	"github.com/Enriquefft/openclaw-kapso-whatsapp/internal/phone"
)

// Verdict represents the outcome of a guard check.
type Verdict int

const (
	Allow Verdict = iota
	Deny
	RateLimited
)

// bucket tracks rate limit state for a single sender.
type bucket struct {
	tokens    int
	windowEnd time.Time
}

// Guard enforces sender allowlist, rate limiting, role resolution, and session isolation.
type Guard struct {
	mode        string
	phoneTo     map[string]string // normalized phone → role
	defaultRole string
	denyMessage string
	rateLimit   int
	rateWindow  time.Duration
	isolate     bool
	now         func() time.Time
	mu          sync.Mutex
	buckets     map[string]*bucket
	sweepEvery  time.Duration
	lastSweep   time.Time
}

// New creates a Guard from the security config. It inverts the role→[]phones
// map into a phone→role lookup for O(1) checks.
func New(cfg config.SecurityConfig) *Guard {
	phoneTo := make(map[string]string)
	for role, phones := range cfg.Roles {
		for _, phone := range phones {
			n := normalize(phone)
			if _, exists := phoneTo[n]; !exists {
				phoneTo[n] = role
			}
		}
	}

	rateWindow := time.Duration(cfg.RateWindow) * time.Second
	// Sweep stale buckets no more than once per window so the map cannot grow
	// unbounded with one entry per distinct sender. Floored so a tiny or zero
	// window does not turn every Check into a full-map scan.
	sweepEvery := rateWindow
	if sweepEvery < time.Minute {
		sweepEvery = time.Minute
	}

	return &Guard{
		mode:        cfg.Mode,
		phoneTo:     phoneTo,
		defaultRole: cfg.DefaultRole,
		denyMessage: cfg.DenyMessage,
		rateLimit:   cfg.RateLimit,
		rateWindow:  rateWindow,
		isolate:     cfg.SessionIsolation,
		now:         time.Now,
		buckets:     make(map[string]*bucket),
		sweepEvery:  sweepEvery,
		lastSweep:   time.Now(),
	}
}

// Check returns Allow, Deny, or RateLimited for the given sender phone number.
func (g *Guard) Check(from string) Verdict {
	n := normalize(from)

	if g.mode == "allowlist" {
		if _, ok := g.phoneTo[n]; !ok {
			return Deny
		}
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.now()

	// Periodically reap expired buckets so idle senders do not accumulate.
	if now.Sub(g.lastSweep) >= g.sweepEvery {
		for k, bk := range g.buckets {
			if now.After(bk.windowEnd) {
				delete(g.buckets, k)
			}
		}
		g.lastSweep = now
	}

	b, ok := g.buckets[n]
	if !ok || now.After(b.windowEnd) {
		g.buckets[n] = &bucket{
			tokens:    g.rateLimit - 1,
			windowEnd: now.Add(g.rateWindow),
		}
		return Allow
	}

	if b.tokens <= 0 {
		return RateLimited
	}
	b.tokens--
	return Allow
}

// Role returns the sender's role. In allowlist mode, returns the mapped role.
// In open mode, returns the mapped role if the sender is in the roles map,
// otherwise returns the default role.
func (g *Guard) Role(from string) string {
	n := normalize(from)
	if role, ok := g.phoneTo[n]; ok {
		return role
	}
	return g.defaultRole
}

// DenyMessage returns the configured denial message.
func (g *Guard) DenyMessage() string {
	return g.denyMessage
}

// SessionKey returns a per-sender session key if isolation is enabled,
// otherwise returns the base key unchanged.
func (g *Guard) SessionKey(baseKey, from string) string {
	if !g.isolate {
		return baseKey
	}
	n := normalize(from)
	return baseKey + "-wa-" + n
}

// normalize strips all non-digit characters (including a leading +) so that
// "+15551234567" and "15551234567" both become "15551234567". This is required
// because the Meta/WhatsApp webhook sends `from` without a leading +, while
// config entries are commonly written with one.
func normalize(s string) string {
	return phone.Normalize(s)
}
