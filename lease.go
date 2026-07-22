package lease

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"sync/atomic"
	"time"
)

var (
	// ErrLeaseHeld is returned when a lease is already held by another holder.
	ErrLeaseHeld = errors.New("lease: lease is held by another holder")
	// ErrEpochMismatch is returned when the holder_epoch does not match.
	ErrEpochMismatch = errors.New("lease: holder_epoch mismatch")
	// ErrLeaseNotFound is returned when the lease record does not exist.
	ErrLeaseNotFound = errors.New("lease: lease not found")
)

// Lease is the core lease interface with four operations:
// contend, renew, release, observe.
//
// Grant is the only credential callers should hold.
// All protected writes must carry Grant.HolderEpoch.
type Lease interface {
	// Contend tries to acquire a lease on resourceID.
	// Returns a Grant on success, or ErrLeaseHeld if someone else holds it.
	Contend(ctx context.Context, resourceID string) (Grant, error)

	// Renew extends the lease identified by grant.
	// Returns an updated Grant on success, or ErrEpochMismatch if lost.
	Renew(ctx context.Context, grant Grant) (Grant, error)

	// Release voluntarily gives up the lease identified by grant.
	// Release is idempotent.
	Release(ctx context.Context, grant Grant) error

	// Observe reads the current lease state without modifying it.
	// Returns ErrLeaseNotFound if no record exists.
	Observe(ctx context.Context, resourceID string) (Grant, error)
}

// leaseImpl implements Lease on top of a Store.
type leaseImpl struct {
	store    Store
	clock    Clock
	holderID string
	ttl      time.Duration

	// epochCounter generates unique holder_epoch values.
	// Seeded with random bits to avoid collisions across restarts.
	epochCounter int64
}

// Option configures a leaseImpl.
type Option func(*leaseImpl)

// WithTTL sets the lease TTL. Defaults to 10s.
func WithTTL(ttl time.Duration) Option {
	return func(l *leaseImpl) { l.ttl = ttl }
}

// WithHolderID sets the holder ID. Defaults to a random UUID-like string.
func WithHolderID(id string) Option {
	return func(l *leaseImpl) { l.holderID = id }
}

// WithClock sets the clock. Defaults to real time.
func WithClock(c Clock) Option {
	return func(l *leaseImpl) { l.clock = c }
}

// New creates a new Lease manager backed by store.
func New(store Store, opts ...Option) Lease {
	l := &leaseImpl{
		store: store,
		clock: realClock{},
		ttl:   10 * time.Second,
	}
	for _, opt := range opts {
		opt(l)
	}
	if l.holderID == "" {
		l.holderID = newHolderID()
	}
	var seed [4]byte
	_, _ = rand.Read(seed[:])
	l.epochCounter = int64(binary.BigEndian.Uint32(seed[:])) << 32
	return l
}

// HolderID returns the configured holder ID.
func (l *leaseImpl) HolderID() string {
	return l.holderID
}

// Contend tries to acquire a lease on resourceID.
func (l *leaseImpl) Contend(ctx context.Context, resourceID string) (Grant, error) {
	epoch := atomic.AddInt64(&l.epochCounter, 1)
	now := l.clock.Now()
	rec := Record{
		ResourceID:  resourceID,
		HolderID:    l.holderID,
		HolderEpoch: epoch,
		ExpiresAt:   now.Add(l.ttl),
		Version:     1,
	}
	got, err := l.store.Insert(ctx, rec)
	if err != nil {
		return Grant{}, err
	}
	return recordToGrant(got), nil
}

// Renew extends the lease identified by grant.
func (l *leaseImpl) Renew(ctx context.Context, grant Grant) (Grant, error) {
	now := l.clock.Now()
	rec := Record{
		ResourceID:  grant.ResourceID,
		HolderID:    grant.HolderID,
		HolderEpoch: grant.HolderEpoch,
		ExpiresAt:   now.Add(l.ttl),
	}
	got, err := l.store.Update(ctx, rec)
	if err != nil {
		return Grant{}, err
	}
	return recordToGrant(got), nil
}

// Release voluntarily gives up the lease.
func (l *leaseImpl) Release(ctx context.Context, grant Grant) error {
	return l.store.Delete(ctx, grant.ResourceID, grant.HolderEpoch)
}

// Observe reads the current lease state.
func (l *leaseImpl) Observe(ctx context.Context, resourceID string) (Grant, error) {
	rec, err := l.store.Get(ctx, resourceID)
	if err != nil {
		return Grant{}, err
	}
	return recordToGrant(rec), nil
}

func recordToGrant(r Record) Grant {
	return Grant{
		ResourceID:  r.ResourceID,
		HolderID:    r.HolderID,
		HolderEpoch: r.HolderEpoch,
		ExpiresAt:   r.ExpiresAt,
	}
}

func newHolderID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return encodeHex(b[:])
}

func encodeHex(b []byte) string {
	const hex = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, v := range b {
		out[i*2] = hex[v>>4]
		out[i*2+1] = hex[v&0x0f]
	}
	return string(out)
}
