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

// Keeper manages background contention and renewal for a single resource.
// Use the package-level Start function to create and start a Keeper.
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

// Start creates a Keeper and begins background contention and renewal for the given resource.
// The keeper will keep trying to acquire the lease when not holding,
// and renew it while holding.
//
// Start runs in a background goroutine; call Stop on the returned Keeper to shut down.
func Start(s Store, resourceID string, opts ...Option) *Keeper {
	l := New(s, opts...).(*Manager)
	k := &Keeper{
		lease: l,
		ttl:   l.ttl,
	}
	k.mu.Lock()
	k.resource = resourceID
	k.grant = Grant{}
	k.held = false
	k.stopCh = make(chan struct{})
	k.doneCh = make(chan struct{})
	stopCh := k.stopCh
	doneCh := k.doneCh
	k.mu.Unlock()

	bgCtx := context.Background()

	go func() {
		defer close(doneCh)

		tryInterval := k.ttl / 2
		renewInterval := k.ttl / 3

		tryTicker := time.NewTicker(tryInterval)
		defer tryTicker.Stop()

		renewTicker := time.NewTicker(renewInterval)
		defer renewTicker.Stop()

		k.tryContend(bgCtx, resourceID)

		for {
			select {
			case <-stopCh:
				k.release(resourceID)
				return
			case <-tryTicker.C:
				if !k.IsHolding() {
					k.tryContend(bgCtx, resourceID)
				}
			case <-renewTicker.C:
				if k.IsHolding() {
					k.tryRenew(bgCtx, resourceID)
				}
			}
		}
	}()

	return k
}

// IsHolding returns whether the keeper currently holds the lease.
func (k *Keeper) IsHolding() bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.held
}

// CurrentGrant returns the current grant and whether the lease is held.
func (k *Keeper) CurrentGrant() (Grant, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.grant, k.held
}

// Stop gracefully shuts down the keeper and releases the lease.
func (k *Keeper) Stop() {
	k.mu.Lock()
	stopCh := k.stopCh
	doneCh := k.doneCh
	k.mu.Unlock()

	if stopCh == nil {
		return
	}
	close(stopCh)
	<-doneCh
}

func (k *Keeper) tryContend(ctx context.Context, resourceID string) {
	grant, err := k.lease.Contend(ctx, resourceID)
	if err != nil {
		return
	}
	k.mu.Lock()
	k.grant = grant
	k.held = true
	k.mu.Unlock()
}

func (k *Keeper) tryRenew(ctx context.Context, resourceID string) {
	k.mu.Lock()
	current := k.grant
	k.mu.Unlock()

	grant, err := k.lease.Renew(ctx, current)
	if err != nil {
		k.mu.Lock()
		k.held = false
		k.grant = Grant{}
		k.mu.Unlock()
		return
	}
	k.mu.Lock()
	k.grant = grant
	k.mu.Unlock()
}

func (k *Keeper) release(resourceID string) {
	k.mu.Lock()
	held := k.held
	grant := k.grant
	k.held = false
	k.grant = Grant{}
	k.mu.Unlock()
	if held {
		_ = k.lease.Release(context.Background(), grant)
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
