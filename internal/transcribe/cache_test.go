package transcribe

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// cacheTestMock is a simple mock that records calls and returns configurable results.
// Unlike mockTranscriber in retry_test.go, this one stores results per-index and
// also tracks what audio bytes were passed so we can verify key differentiation.
type cacheTestMock struct {
	results []mockResult
	calls   int
}

func (m *cacheTestMock) Transcribe(_ context.Context, _ []byte, _ string) (string, error) {
	i := m.calls
	if i >= len(m.results) {
		i = len(m.results) - 1
	}
	m.calls++
	return m.results[i].text, m.results[i].err
}

func TestCacheTranscriber(t *testing.T) {
	ctx := context.Background()
	audio1 := []byte("audio-bytes-one")
	audio2 := []byte("audio-bytes-two")

	tests := []struct {
		name      string
		run       func(t *testing.T, ct *cacheTranscriber, mock *cacheTestMock)
		wantCalls int
	}{
		{
			name: "cache miss then hit",
			run: func(t *testing.T, ct *cacheTranscriber, mock *cacheTestMock) {
				// First call: cache miss — inner must be called.
				text1, err := ct.Transcribe(ctx, audio1, "audio/ogg")
				if err != nil {
					t.Fatalf("first call error: %v", err)
				}
				if text1 != "hello" {
					t.Errorf("first call = %q, want %q", text1, "hello")
				}

				// Second call with same audio: cache hit — inner must NOT be called again.
				text2, err := ct.Transcribe(ctx, audio1, "audio/ogg")
				if err != nil {
					t.Fatalf("second call error: %v", err)
				}
				if text2 != "hello" {
					t.Errorf("second call = %q, want %q", text2, "hello")
				}
			},
			wantCalls: 1,
		},
		{
			name: "TTL expiry causes fresh call",
			run: func(t *testing.T, ct *cacheTranscriber, mock *cacheTestMock) {
				// First call: populates cache.
				_, err := ct.Transcribe(ctx, audio1, "audio/ogg")
				if err != nil {
					t.Fatalf("first call error: %v", err)
				}

				// Advance clock past TTL.
				baseNow := time.Now()
				ct.nowFunc = func() time.Time {
					return baseNow.Add(2 * time.Hour) // well past 1h TTL
				}

				// Second call: cache expired — inner must be called again.
				_, err = ct.Transcribe(ctx, audio1, "audio/ogg")
				if err != nil {
					t.Fatalf("second call after TTL error: %v", err)
				}
			},
			wantCalls: 2,
		},
		{
			name: "error not cached",
			run: func(t *testing.T, ct *cacheTranscriber, mock *cacheTestMock) {
				// First call: inner returns error — must NOT be cached.
				_, err := ct.Transcribe(ctx, audio1, "audio/ogg")
				if err == nil {
					t.Fatal("expected error on first call, got nil")
				}

				// Second call: inner should be called again (error not cached).
				text, err := ct.Transcribe(ctx, audio1, "audio/ogg")
				if err != nil {
					t.Fatalf("second call error: %v", err)
				}
				if text != "recovered" {
					t.Errorf("second call = %q, want %q", text, "recovered")
				}
			},
			wantCalls: 2,
		},
		{
			name: "different audio different keys",
			run: func(t *testing.T, ct *cacheTranscriber, mock *cacheTestMock) {
				// Call with audio1.
				_, err := ct.Transcribe(ctx, audio1, "audio/ogg")
				if err != nil {
					t.Fatalf("audio1 call error: %v", err)
				}

				// Call with audio2 — different bytes, must call inner again.
				_, err = ct.Transcribe(ctx, audio2, "audio/ogg")
				if err != nil {
					t.Fatalf("audio2 call error: %v", err)
				}
			},
			wantCalls: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var mock *cacheTestMock
			switch tc.name {
			case "cache miss then hit":
				mock = &cacheTestMock{results: []mockResult{
					{text: "hello", err: nil},
				}}
			case "TTL expiry causes fresh call":
				mock = &cacheTestMock{results: []mockResult{
					{text: "hello", err: nil},
					{text: "hello again", err: nil},
				}}
			case "error not cached":
				mock = &cacheTestMock{results: []mockResult{
					{text: "", err: errors.New("transcription failed")},
					{text: "recovered", err: nil},
				}}
			case "different audio different keys":
				mock = &cacheTestMock{results: []mockResult{
					{text: "text from audio1", err: nil},
					{text: "text from audio2", err: nil},
				}}
			default:
				t.Fatalf("unknown test case: %s", tc.name)
			}

			ct := newCacheTranscriber(mock, 1*time.Hour)

			tc.run(t, ct, mock)

			if mock.calls != tc.wantCalls {
				t.Errorf("inner called %d times, want %d", mock.calls, tc.wantCalls)
			}
		})
	}
}

// TestCacheEvictionBounded verifies the cache cannot grow past maxEntries no
// matter how many unique audio messages arrive.
func TestCacheEvictionBounded(t *testing.T) {
	ctx := context.Background()
	mock := &cacheTestMock{results: []mockResult{{text: "ok", err: nil}}}
	ct := newCacheTranscriber(mock, time.Hour)
	ct.maxEntries = 4

	for i := 0; i < 50; i++ {
		if _, err := ct.Transcribe(ctx, []byte(fmt.Sprintf("audio-%d", i)), "audio/ogg"); err != nil {
			t.Fatalf("Transcribe %d: %v", i, err)
		}
	}

	ct.mu.Lock()
	n := len(ct.items)
	ct.mu.Unlock()
	if n > ct.maxEntries {
		t.Fatalf("cache holds %d entries, want <= %d", n, ct.maxEntries)
	}
}

// TestCacheEvictsExpiredOnWrite verifies expired entries are dropped when a new
// entry is stored, rather than lingering forever.
func TestCacheEvictsExpiredOnWrite(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(0, 0)
	mock := &cacheTestMock{results: []mockResult{{text: "ok", err: nil}}}
	ct := newCacheTranscriber(mock, time.Minute)
	ct.maxEntries = 100
	ct.nowFunc = func() time.Time { return now }

	if _, err := ct.Transcribe(ctx, []byte("A"), "audio/ogg"); err != nil {
		t.Fatalf("store A: %v", err)
	}

	// Advance past A's TTL, then store B — storing must evict the expired A.
	now = now.Add(2 * time.Minute)
	if _, err := ct.Transcribe(ctx, []byte("B"), "audio/ogg"); err != nil {
		t.Fatalf("store B: %v", err)
	}

	ct.mu.Lock()
	_, hasA := ct.items[ct.cacheKey([]byte("A"))]
	_, hasB := ct.items[ct.cacheKey([]byte("B"))]
	ct.mu.Unlock()
	if hasA {
		t.Error("expired entry A was not evicted on write")
	}
	if !hasB {
		t.Error("fresh entry B is missing")
	}
}
