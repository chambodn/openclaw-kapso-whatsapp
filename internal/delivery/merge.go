package delivery

import (
	"context"
	"log"
	"sync"
	"time"
)

// Merge fans in multiple Sources with message-ID deduplication.
type Merge struct {
	Sources []Source
	nowFunc func() time.Time // injectable clock for tests
	seen    sync.Map         // Event.ID -> time.Time (first-seen)
}

// now returns the current time via the injected clock, defaulting to time.Now
// so a zero-value Merge constructed via struct literal works without setup.
func (m *Merge) now() time.Time {
	if m.nowFunc != nil {
		return m.nowFunc()
	}
	return time.Now()
}

// Run starts all sources concurrently, deduplicates by Event.ID, and forwards
// unique events to out. It returns when all sources have finished.
func (m *Merge) Run(ctx context.Context, out chan<- Event) error {
	ch := make(chan Event, 64)

	var wg sync.WaitGroup
	for _, src := range m.Sources {
		wg.Add(1)
		go func(s Source) {
			defer wg.Done()
			if err := s.Run(ctx, ch); err != nil && ctx.Err() == nil {
				log.Printf("source error: %v", err)
			}
		}(src)
	}

	// Close ch when all sources are done.
	go func() {
		wg.Wait()
		close(ch)
	}()

	for evt := range ch {
		if _, loaded := m.seen.LoadOrStore(evt.ID, m.now()); loaded {
			log.Printf("merge: skipping duplicate message %s", evt.ID)
			continue
		}
		out <- evt
	}

	close(out)
	return nil
}

// StartCleanup periodically evicts dedup entries older than ttl to bound memory
// usage. Unlike a wholesale wipe, age-based eviction keeps each message ID for
// at least ttl after it was first seen, so a source (e.g. the poller) cannot
// re-deliver a still-recent message just because a fixed cleanup boundary
// happened to land moments after the ID was recorded.
func (m *Merge) StartCleanup(ctx context.Context, ttl time.Duration) {
	ticker := time.NewTicker(ttl)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.sweep(m.now(), ttl)
		}
	}
}

// sweep deletes dedup entries first seen before now-ttl.
func (m *Merge) sweep(now time.Time, ttl time.Duration) {
	cutoff := now.Add(-ttl)
	m.seen.Range(func(key, value any) bool {
		if seenAt, ok := value.(time.Time); ok && seenAt.Before(cutoff) {
			m.seen.Delete(key)
		}
		return true
	})
}
