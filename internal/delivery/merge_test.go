package delivery

import (
	"context"
	"testing"
	"time"
)

// sliceSource is a test Source that emits a fixed list of events then returns.
type sliceSource struct{ events []Event }

func (s sliceSource) Run(ctx context.Context, out chan<- Event) error {
	for _, e := range s.events {
		select {
		case out <- e:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// TestMergeRunDeduplicates verifies a message ID emitted by two sources is
// forwarded exactly once.
func TestMergeRunDeduplicates(t *testing.T) {
	m := &Merge{Sources: []Source{
		sliceSource{events: []Event{{ID: "a"}, {ID: "b"}}},
		sliceSource{events: []Event{{ID: "a"}, {ID: "c"}}}, // "a" is a duplicate
	}}

	out := make(chan Event, 16)
	if err := m.Run(context.Background(), out); err != nil {
		t.Fatalf("Run: %v", err)
	}

	counts := map[string]int{}
	for e := range out { // Run closes out when sources finish
		counts[e.ID]++
	}
	if len(counts) != 3 {
		t.Fatalf("expected 3 unique IDs, got %d: %v", len(counts), counts)
	}
	for id, c := range counts {
		if c != 1 {
			t.Errorf("ID %q emitted %d times, want 1", id, c)
		}
	}
}

// TestMergeSweepEvictsOldEntries verifies age-based eviction removes entries
// older than the TTL while keeping recent ones — and never wipes wholesale.
func TestMergeSweepEvictsOldEntries(t *testing.T) {
	m := &Merge{}
	m.seen.Store("old", time.Unix(0, 0))
	m.seen.Store("recent", time.Unix(1000, 0))

	// now=1000, ttl=500 -> cutoff=500; entries first seen before 500 are evicted.
	m.sweep(time.Unix(1000, 0), 500*time.Second)

	if _, ok := m.seen.Load("old"); ok {
		t.Error("entry first seen at t=0 should have been evicted")
	}
	if _, ok := m.seen.Load("recent"); !ok {
		t.Error("entry first seen at t=1000 should be retained")
	}
}
