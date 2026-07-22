package lease

import (
	"context"
	"errors"
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

// Record is the shared storage representation of a lease.
type Record struct {
	ResourceID  string            `bson:"_id"`
	HolderID    string            `bson:"holder_id"`
	HolderEpoch int64             `bson:"holder_epoch"`
	ExpiresAt   time.Time         `bson:"expires_at"`
	Version     int64             `bson:"version"`
	Metadata    map[string]string `bson:"metadata,omitempty"`
}

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
