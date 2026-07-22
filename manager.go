package lease

import (
	"context"
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

// Clock abstracts time for testability.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }
