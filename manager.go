package lease

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"
)

// Store is the storage backend interface.
// Implementations must provide atomic single-document CAS semantics.
//
// The four methods map directly to the four lease operations and mirror
// their error semantics (ErrLeaseHeld, ErrEpochMismatch, ErrLeaseNotFound).
type Store interface {
	// Insert inserts a new lease record only if none exists or the existing one is expired/free.
	// Returns ErrLeaseHeld if a valid (non-expired) lease is held by someone else.
	Insert(ctx context.Context, rec Resource) (Resource, error)

	// Update extends the lease identified by HolderEpoch.
	// Returns ErrEpochMismatch if the epoch does not match.
	Update(ctx context.Context, rec Resource) (Resource, error)

	// Delete clears holder fields for the given epoch (soft delete — record remains).
	// Delete is idempotent: calling with a stale epoch does not error.
	Delete(ctx context.Context, resourceID string, holderEpoch int64) error

	// Get reads the current record for a resource.
	// Returns ErrLeaseNotFound if no record exists.
	Get(ctx context.Context, resourceID string) (Resource, error)
}

// Manager implements Lease on top of a Store.
type Manager struct {
	store    Store
	holderID string
	ttl      time.Duration

	// epoch generates unique holder_epoch values.
	// Seeded with random bits to avoid collisions across restarts.
	epoch int64
}

// Option configures a manger.
type Option func(*Manager)

// WithTTL sets the lease TTL. Defaults to 10s.
func WithTTL(ttl time.Duration) Option {
	return func(l *Manager) { l.ttl = ttl }
}

// WithHolderID sets the holder ID. Defaults to a random UUID-like string.
func WithHolderID(id string) Option {
	return func(l *Manager) { l.holderID = id }
}

// New creates a new Lease backed by store.
func New(s Store, opts ...Option) Lease {
	return newManger(s, opts...)
}

func newManger(s Store, opts ...Option) *Manager {
	l := &Manager{
		store: s,
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
	l.epoch = int64(binary.BigEndian.Uint32(seed[:])) << 32
	return l
}

// HolderID returns the configured holder ID.
func (l *Manager) HolderID() string {
	return l.holderID
}

// Contend tries to acquire a lease on resourceID.
func (l *Manager) Contend(ctx context.Context, resourceID string) (Grant, error) {
	epoch := atomic.AddInt64(&l.epoch, 1)
	now := time.Now()
	rec := Resource{
		ID:          resourceID,
		HolderID:    l.holderID,
		HolderEpoch: epoch,
		ExpiresAt:   now.Add(l.ttl),
		Version:     1,
	}
	got, err := l.store.Insert(ctx, rec)
	if err != nil {
		return Grant{}, err
	}
	return resourceToGrant(got), nil
}

// Renew extends the lease identified by grant.
func (l *Manager) Renew(ctx context.Context, grant Grant) (Grant, error) {
	now := time.Now()
	rec := Resource{
		ID:          grant.ResourceID,
		HolderID:    grant.HolderID,
		HolderEpoch: grant.HolderEpoch,
		ExpiresAt:   now.Add(l.ttl),
	}
	got, err := l.store.Update(ctx, rec)
	if err != nil {
		return Grant{}, err
	}
	return resourceToGrant(got), nil
}

// Release voluntarily gives up the lease.
func (l *Manager) Release(ctx context.Context, grant Grant) error {
	return l.store.Delete(ctx, grant.ResourceID, grant.HolderEpoch)
}

// Observe reads the current lease state.
func (l *Manager) Observe(ctx context.Context, resourceID string) (Grant, error) {
	rec, err := l.store.Get(ctx, resourceID)
	if err != nil {
		return Grant{}, err
	}
	return resourceToGrant(rec), nil
}

// --- Keeper (with background mode) ---

// Keeper wraps a Lease and manages background contention and renewal.
// Use the package-level Start function to begin background mode for a resource.
type Keeper struct {
	lease *Manager
	ttl   time.Duration

	mu       sync.Mutex
	resource string
	grant    Grant
	held     bool
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// Start creates a Manager and begins background contention and renewal for the given resource.
// The manager will keep trying to acquire the lease when not holding,
// and renew it while holding.
//
// Start runs in a background goroutine; call Stop on the returned Manager to shut down.
func Start(ctx context.Context, s Store, resourceID string, opts ...Option) *Keeper {
	m := NewKeeper(s, opts...)
	m.mu.Lock()
	m.resource = resourceID
	m.grant = Grant{}
	m.held = false
	m.stopCh = make(chan struct{})
	m.doneCh = make(chan struct{})
	stopCh := m.stopCh
	doneCh := m.doneCh
	m.mu.Unlock()

	go func() {
		defer close(doneCh)

		tryInterval := m.ttl / 2
		renewInterval := m.ttl / 3

		tryTicker := time.NewTicker(tryInterval)
		defer tryTicker.Stop()

		renewTicker := time.NewTicker(renewInterval)
		defer renewTicker.Stop()

		m.tryContend(ctx, resourceID)

		for {
			select {
			case <-ctx.Done():
				m.release(resourceID)
				return
			case <-stopCh:
				m.release(resourceID)
				return
			case <-tryTicker.C:
				if !m.isHeld() {
					m.tryContend(ctx, resourceID)
				}
			case <-renewTicker.C:
				if m.isHeld() {
					m.tryRenew(ctx, resourceID)
				}
			}
		}
	}()

	return m
}

// NewKeeper creates a new Keeper with background mode support.
func NewKeeper(s Store, opts ...Option) *Keeper {
	l := newManger(s, opts...)
	return &Keeper{
		lease: l,
		ttl:   l.ttl,
	}
}

// Lease returns the underlying Lease interface.
func (m *Keeper) Lease() Lease {
	return m.lease
}

// Stop gracefully shuts down the background manager and releases the lease.
func (m *Keeper) Stop() {
	m.mu.Lock()
	stopCh := m.stopCh
	doneCh := m.doneCh
	m.mu.Unlock()

	if stopCh == nil {
		return
	}
	close(stopCh)
	<-doneCh
}

// Grant returns the current grant and whether the lease is held.
func (m *Keeper) Grant() (Grant, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.grant, m.held
}

func (m *Keeper) isHeld() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.held
}

func (m *Keeper) tryContend(ctx context.Context, resourceID string) {
	grant, err := m.lease.Contend(ctx, resourceID)
	if err != nil {
		return
	}
	m.mu.Lock()
	m.grant = grant
	m.held = true
	m.mu.Unlock()
}

func (m *Keeper) tryRenew(ctx context.Context, resourceID string) {
	m.mu.Lock()
	current := m.grant
	m.mu.Unlock()

	grant, err := m.lease.Renew(ctx, current)
	if err != nil {
		m.mu.Lock()
		m.held = false
		m.grant = Grant{}
		m.mu.Unlock()
		return
	}
	m.mu.Lock()
	m.grant = grant
	m.mu.Unlock()
}

func (m *Keeper) release(resourceID string) {
	m.mu.Lock()
	held := m.held
	grant := m.grant
	m.held = false
	m.grant = Grant{}
	m.mu.Unlock()
	if held {
		_ = m.lease.Release(context.Background(), grant)
	}
}

// --- helpers ---

func resourceToGrant(r Resource) Grant {
	return Grant{
		ResourceID:  r.ID,
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
