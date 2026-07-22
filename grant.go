package lease

import "time"

// Grant is the caller's proof of lease ownership.
// It is the only credential callers should hold; all protected writes
// must carry HolderEpoch for fencing.
type Grant struct {
	ResourceID  string
	HolderID    string
	HolderEpoch int64
	ExpiresAt   time.Time
}

// IsValid returns true if the grant is still held at time t.
func (g Grant) IsValid(t time.Time) bool {
	return g.HolderEpoch != 0 && t.Before(g.ExpiresAt)
}
