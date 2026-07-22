package lease

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"
)

// Record is the shared storage representation of a lease.
// It maps to a single MongoDB document.
type Record struct {
	ResourceID  string            `bson:"_id"`
	HolderID    string            `bson:"holder_id"`
	HolderEpoch int64             `bson:"holder_epoch"`
	ExpiresAt   time.Time         `bson:"expires_at"`
	Version     int64             `bson:"version"`
	Metadata    map[string]string `bson:"metadata,omitempty"`
}

// Store is the storage backend interface.
// Implementations must provide atomic single-document CAS semantics.
//
// The four methods map directly to the four lease operations and mirror
// their error semantics (ErrLeaseHeld, ErrEpochMismatch, ErrLeaseNotFound).
type Store interface {
	// Insert inserts a new lease record only if none exists or the existing one is expired/free.
	// Returns ErrLeaseHeld if a valid (non-expired) lease is held by someone else.
	Insert(ctx context.Context, rec Record) (Record, error)

	// Update extends the lease identified by HolderEpoch.
	// Returns ErrEpochMismatch if the epoch does not match.
	Update(ctx context.Context, rec Record) (Record, error)

	// Delete clears holder fields for the given epoch (soft delete — record remains).
	// Delete is idempotent: calling with a stale epoch does not error.
	Delete(ctx context.Context, resourceID string, holderEpoch int64) error

	// Get reads the current record for a resource.
	// Returns ErrLeaseNotFound if no record exists.
	Get(ctx context.Context, resourceID string) (Record, error)
}

// manger implements Lease on top of a Store.
type manger struct {
	store    Store
	holderID string
	ttl      time.Duration

	// epochCounter generates unique holder_epoch values.
	// Seeded with random bits to avoid collisions across restarts.
	epochCounter int64
}

// Option configures a manger.
type Option func(*manger)

// WithTTL sets the lease TTL. Defaults to 10s.
func WithTTL(ttl time.Duration) Option {
	return func(l *manger) { l.ttl = ttl }
}

// WithHolderID sets the holder ID. Defaults to a random UUID-like string.
func WithHolderID(id string) Option {
	return func(l *manger) { l.holderID = id }
}

// New creates a new Lease manager backed by store.
func New(store Store, opts ...Option) Lease {
	l := &manger{
		store: store,
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
func (l *manger) HolderID() string {
	return l.holderID
}

// Contend tries to acquire a lease on resourceID.
func (l *manger) Contend(ctx context.Context, resourceID string) (Grant, error) {
	epoch := atomic.AddInt64(&l.epochCounter, 1)
	now := time.Now()
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
func (l *manger) Renew(ctx context.Context, grant Grant) (Grant, error) {
	now := time.Now()
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
func (l *manger) Release(ctx context.Context, grant Grant) error {
	return l.store.Delete(ctx, grant.ResourceID, grant.HolderEpoch)
}

// Observe reads the current lease state.
func (l *manger) Observe(ctx context.Context, resourceID string) (Grant, error) {
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

// --- Background mode ---

// BackgroundHolder manages background contention and renewal for a single resource.
// It keeps trying to acquire the lease when not holding, and renews it while holding.
// Start is not part of the Lease interface — it's a convenience for
// applications that want a self-driving lease holder.
type BackgroundHolder struct {
	lease         Lease
	resourceID    string
	renewInterval time.Duration
	tryInterval   time.Duration

	mu    sync.Mutex
	grant Grant
	held  bool

	stopCh chan struct{}
	doneCh chan struct{}
}

// NewBackgroundHolder creates a background holder for a single resource.
// renewInterval defaults to TTL/3; tryInterval defaults to TTL/2.
func (l *manger) NewBackgroundHolder(resourceID string) *BackgroundHolder {
	return &BackgroundHolder{
		lease:         l,
		resourceID:    resourceID,
		renewInterval: l.ttl / 3,
		tryInterval:   l.ttl / 2,
	}
}

// Start begins background contention and renewal.
// It blocks until Stop is called or the context is cancelled.
func (b *BackgroundHolder) Start(ctx context.Context) {
	b.stopCh = make(chan struct{})
	b.doneCh = make(chan struct{})
	defer close(b.doneCh)

	tryTicker := time.NewTicker(b.tryInterval)
	defer tryTicker.Stop()

	renewTicker := time.NewTicker(b.renewInterval)
	defer renewTicker.Stop()

	// Try immediately
	b.tryContend(ctx)

	for {
		select {
		case <-ctx.Done():
			b.release()
			return
		case <-b.stopCh:
			b.release()
			return
		case <-tryTicker.C:
			if !b.isHeld() {
				b.tryContend(ctx)
			}
		case <-renewTicker.C:
			if b.isHeld() {
				b.tryRenew(ctx)
			}
		}
	}
}

// Stop gracefully shuts down the background holder and releases the lease.
func (b *BackgroundHolder) Stop() {
	close(b.stopCh)
	<-b.doneCh
}

// Grant returns the current grant and whether we hold the lease.
func (b *BackgroundHolder) Grant() (Grant, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.grant, b.held
}

func (b *BackgroundHolder) isHeld() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.held
}

func (b *BackgroundHolder) tryContend(ctx context.Context) {
	grant, err := b.lease.Contend(ctx, b.resourceID)
	if err != nil {
		return
	}
	b.mu.Lock()
	b.grant = grant
	b.held = true
	b.mu.Unlock()
}

func (b *BackgroundHolder) tryRenew(ctx context.Context) {
	b.mu.Lock()
	current := b.grant
	b.mu.Unlock()

	grant, err := b.lease.Renew(ctx, current)
	if err != nil {
		b.mu.Lock()
		b.held = false
		b.grant = Grant{}
		b.mu.Unlock()
		return
	}
	b.mu.Lock()
	b.grant = grant
	b.mu.Unlock()
}

func (b *BackgroundHolder) release() {
	b.mu.Lock()
	held := b.held
	grant := b.grant
	b.held = false
	b.grant = Grant{}
	b.mu.Unlock()
	if held {
		_ = b.lease.Release(context.Background(), grant)
	}
}
