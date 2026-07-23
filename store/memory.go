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
	data map[string]*lease.Resource
}

// NewMemory creates a new in-memory store.
func NewMemory() *Memory {
	return &Memory{data: make(map[string]*lease.Resource)}
}

// Insert inserts a new lease record only if none exists or the existing one is expired/free.
func (s *Memory) Insert(_ context.Context, rec lease.Resource) (lease.Resource, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.data[rec.ID]
	now := time.Now()
	if ok && existing.HolderEpoch != 0 && existing.ExpiresAt.After(now) {
		return lease.Resource{}, lease.ErrLeaseHeld
	}
	newRec := rec
	newRec.Version = 1
	if ok {
		newRec.Version = existing.Version + 1
	}
	stored := newRec
	s.data[rec.ID] = &stored
	return stored, nil
}

// Update extends the lease identified by HolderEpoch.
func (s *Memory) Update(_ context.Context, rec lease.Resource) (lease.Resource, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.data[rec.ID]
	if !ok || existing.HolderEpoch != rec.HolderEpoch {
		return lease.Resource{}, lease.ErrEpochMismatch
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
func (s *Memory) Get(_ context.Context, resourceID string) (lease.Resource, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.data[resourceID]
	if !ok {
		return lease.Resource{}, lease.ErrLeaseNotFound
	}
	return *rec, nil
}
