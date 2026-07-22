package lease

import (
	"context"
	"sync"
	"time"
)

// fakeClock is a manually-advanced clock for testing.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// memoryStore is an in-memory Store implementation for testing.
type memoryStore struct {
	mu   sync.Mutex
	data map[string]*Record
}

func newMemoryStore() *memoryStore {
	return &memoryStore{data: make(map[string]*Record)}
}

func (s *memoryStore) Insert(_ context.Context, rec Record) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.data[rec.ResourceID]
	now := time.Now()
	if ok && existing.HolderEpoch != 0 && existing.ExpiresAt.After(now) {
		return Record{}, ErrLeaseHeld
	}
	newRec := rec
	newRec.Version = 1
	if ok {
		newRec.Version = existing.Version + 1
	}
	stored := newRec
	s.data[rec.ResourceID] = &stored
	return stored, nil
}

func (s *memoryStore) Update(_ context.Context, rec Record) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.data[rec.ResourceID]
	if !ok || existing.HolderEpoch != rec.HolderEpoch {
		return Record{}, ErrEpochMismatch
	}
	existing.ExpiresAt = rec.ExpiresAt
	existing.HolderID = rec.HolderID
	existing.Version++
	return *existing, nil
}

func (s *memoryStore) Delete(_ context.Context, resourceID string, holderEpoch int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.data[resourceID]
	if !ok || existing.HolderEpoch != holderEpoch {
		return nil
	}
	existing.HolderID = ""
	existing.HolderEpoch = 0
	existing.ExpiresAt = time.Time{}
	existing.Version++
	return nil
}

func (s *memoryStore) Get(_ context.Context, resourceID string) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.data[resourceID]
	if !ok {
		return Record{}, ErrLeaseNotFound
	}
	return *rec, nil
}

// memoryStoreWithClock is a memoryStore that uses a fakeClock for expiry checks.
type memoryStoreWithClock struct {
	*memoryStore
	clock *fakeClock
}

func newMemoryStoreWithClock(c *fakeClock) *memoryStoreWithClock {
	return &memoryStoreWithClock{
		memoryStore: newMemoryStore(),
		clock:       c,
	}
}

func (s *memoryStoreWithClock) Insert(_ context.Context, rec Record) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.data[rec.ResourceID]
	now := s.clock.Now()
	if ok && existing.HolderEpoch != 0 && existing.ExpiresAt.After(now) {
		return Record{}, ErrLeaseHeld
	}
	newRec := rec
	newRec.Version = 1
	if ok {
		newRec.Version = existing.Version + 1
	}
	stored := newRec
	s.data[rec.ResourceID] = &stored
	return stored, nil
}
