package store

import (
	"context"
	"sync"
	"time"

	"github.com/a8m/lease"
)

// Memory is an in-memory lease.Store implementation, primarily for testing.
type Memory struct {
	mu   sync.Mutex
	data map[string]*lease.Record
}

// NewMemory creates a new in-memory store.
func NewMemory() *Memory {
	return &Memory{data: make(map[string]*lease.Record)}
}

// Insert inserts a new lease record only if none exists or the existing one is expired/free.
func (s *Memory) Insert(_ context.Context, rec lease.Record) (lease.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.data[rec.ResourceID]
	now := time.Now()
	if ok && existing.HolderEpoch != 0 && existing.ExpiresAt.After(now) {
		return lease.Record{}, lease.ErrLeaseHeld
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

// Update extends the lease identified by HolderEpoch.
func (s *Memory) Update(_ context.Context, rec lease.Record) (lease.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.data[rec.ResourceID]
	if !ok || existing.HolderEpoch != rec.HolderEpoch {
		return lease.Record{}, lease.ErrEpochMismatch
	}
	existing.ExpiresAt = rec.ExpiresAt
	existing.HolderID = rec.HolderID
	existing.Version++
	return *existing, nil
}

// Delete clears holder fields for the given epoch (soft delete).
func (s *Memory) Delete(_ context.Context, resourceID string, holderEpoch int64) error {
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

// Get reads the current record for a resource.
func (s *Memory) Get(_ context.Context, resourceID string) (lease.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.data[resourceID]
	if !ok {
		return lease.Record{}, lease.ErrLeaseNotFound
	}
	return *rec, nil
}
