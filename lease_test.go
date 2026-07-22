package lease

import (
	"context"
	"testing"
	"time"
)

func TestContendAndObserve(t *testing.T) {
	store := newMemoryStore()
	m := New(store, WithHolderID("worker-1"), WithTTL(30*time.Second))
	ctx := context.Background()

	grant, err := m.Contend(ctx, "res-1")
	if err != nil {
		t.Fatalf("contend failed: %v", err)
	}
	if grant.HolderID != "worker-1" {
		t.Errorf("holder id = %q, want worker-1", grant.HolderID)
	}
	if grant.HolderEpoch == 0 {
		t.Error("holder epoch should not be zero")
	}
	if grant.ResourceID != "res-1" {
		t.Errorf("resource id = %q, want res-1", grant.ResourceID)
	}

	// Observe should see it
	observed, err := m.Observe(ctx, "res-1")
	if err != nil {
		t.Fatalf("observe failed: %v", err)
	}
	if observed.HolderID != "worker-1" {
		t.Errorf("observe holder = %q, want worker-1", observed.HolderID)
	}
}

func TestContendConflict(t *testing.T) {
	store := newMemoryStore()
	m1 := New(store, WithHolderID("worker-1"), WithTTL(30*time.Second))
	m2 := New(store, WithHolderID("worker-2"), WithTTL(30*time.Second))
	ctx := context.Background()

	_, err := m1.Contend(ctx, "res-1")
	if err != nil {
		t.Fatalf("worker-1 contend failed: %v", err)
	}

	_, err = m2.Contend(ctx, "res-1")
	if err != ErrLeaseHeld {
		t.Errorf("worker-2 contend err = %v, want ErrLeaseHeld", err)
	}
}

func TestRenewSuccess(t *testing.T) {
	store := newMemoryStore()
	m := New(store, WithHolderID("worker-1"), WithTTL(30*time.Second))
	ctx := context.Background()

	grant, err := m.Contend(ctx, "res-1")
	if err != nil {
		t.Fatalf("contend failed: %v", err)
	}

	renewed, err := m.Renew(ctx, grant)
	if err != nil {
		t.Fatalf("renew failed: %v", err)
	}
	if renewed.HolderEpoch != grant.HolderEpoch {
		t.Errorf("renew epoch changed: %d vs %d", renewed.HolderEpoch, grant.HolderEpoch)
	}
	if !renewed.ExpiresAt.After(grant.ExpiresAt) {
		t.Error("renewed expires_at should be later")
	}
}

func TestRenewEpochMismatch(t *testing.T) {
	store := newMemoryStore()
	m1 := New(store, WithHolderID("worker-1"), WithTTL(30*time.Second))
	m2 := New(store, WithHolderID("worker-2"), WithTTL(30*time.Second))
	ctx := context.Background()

	grant1, err := m1.Contend(ctx, "res-1")
	if err != nil {
		t.Fatalf("worker-1 contend failed: %v", err)
	}

	// worker-2 tries to renew with worker-1's epoch - should fail
	badGrant := Grant{
		ResourceID:  "res-1",
		HolderID:    "worker-2",
		HolderEpoch: grant1.HolderEpoch + 999,
		ExpiresAt:   time.Now().Add(time.Minute),
	}
	_, err = m2.Renew(ctx, badGrant)
	if err != ErrEpochMismatch {
		t.Errorf("renew with bad epoch err = %v, want ErrEpochMismatch", err)
	}
}

func TestReleaseAndRecontend(t *testing.T) {
	store := newMemoryStore()
	m1 := New(store, WithHolderID("worker-1"), WithTTL(30*time.Second))
	m2 := New(store, WithHolderID("worker-2"), WithTTL(30*time.Second))
	ctx := context.Background()

	grant1, err := m1.Contend(ctx, "res-1")
	if err != nil {
		t.Fatalf("worker-1 contend failed: %v", err)
	}

	err = m1.Release(ctx, grant1)
	if err != nil {
		t.Fatalf("worker-1 release failed: %v", err)
	}

	// After release, worker-2 should be able to contend
	grant2, err := m2.Contend(ctx, "res-1")
	if err != nil {
		t.Fatalf("worker-2 contend after release failed: %v", err)
	}
	if grant2.HolderID != "worker-2" {
		t.Errorf("new holder = %q, want worker-2", grant2.HolderID)
	}
}

func TestReleaseIdempotent(t *testing.T) {
	store := newMemoryStore()
	m := New(store, WithHolderID("worker-1"), WithTTL(30*time.Second))
	ctx := context.Background()

	grant, err := m.Contend(ctx, "res-1")
	if err != nil {
		t.Fatalf("contend failed: %v", err)
	}

	if err := m.Release(ctx, grant); err != nil {
		t.Fatalf("first release failed: %v", err)
	}
	// Second release should not error
	if err := m.Release(ctx, grant); err != nil {
		t.Fatalf("second release failed: %v", err)
	}
}

func TestExpiredLeaseCanBeTaken(t *testing.T) {
	clock := newFakeClock()
	store := newMemoryStoreWithClock(clock)
	m1 := New(store, WithHolderID("worker-1"), WithTTL(10*time.Second), WithClock(clock))
	m2 := New(store, WithHolderID("worker-2"), WithTTL(10*time.Second), WithClock(clock))
	ctx := context.Background()

	_, err := m1.Contend(ctx, "res-1")
	if err != nil {
		t.Fatalf("worker-1 contend failed: %v", err)
	}

	// m2 should not be able to take it yet
	_, err = m2.Contend(ctx, "res-1")
	if err != ErrLeaseHeld {
		t.Errorf("before expiry: m2 err = %v, want ErrLeaseHeld", err)
	}

	// Advance past expiry
	clock.Add(15 * time.Second)

	// Now m2 should be able to contend
	_, err = m2.Contend(ctx, "res-1")
	if err != nil {
		t.Fatalf("after expiry: m2 contend failed: %v", err)
	}
}

func TestObserveNotFound(t *testing.T) {
	store := newMemoryStore()
	m := New(store, WithHolderID("worker-1"), WithTTL(30*time.Second))
	ctx := context.Background()

	_, err := m.Observe(ctx, "nonexistent")
	if err != ErrLeaseNotFound {
		t.Errorf("observe err = %v, want ErrLeaseNotFound", err)
	}
}

func TestGrantIsValid(t *testing.T) {
	now := time.Now()
	g := Grant{
		ResourceID:  "r",
		HolderID:    "h",
		HolderEpoch: 1,
		ExpiresAt:   now.Add(time.Minute),
	}
	if !g.IsValid(now) {
		t.Error("grant should be valid")
	}
	if !g.IsValid(now.Add(30 * time.Second)) {
		t.Error("grant should still be valid after 30s")
	}
	if g.IsValid(now.Add(2 * time.Minute)) {
		t.Error("grant should be expired after 2m")
	}

	// Zero epoch is never valid
	g2 := Grant{ExpiresAt: now.Add(time.Minute)}
	if g2.IsValid(now) {
		t.Error("grant with zero epoch should not be valid")
	}
}

func TestHolderEpochUniqueAcrossContends(t *testing.T) {
	store := newMemoryStore()
	m := New(store, WithHolderID("worker-1"), WithTTL(30*time.Second))
	ctx := context.Background()

	g1, err := m.Contend(ctx, "res-1")
	if err != nil {
		t.Fatal(err)
	}
	// Release and re-contend, epoch should change
	if err := m.Release(ctx, g1); err != nil {
		t.Fatal(err)
	}
	g2, err := m.Contend(ctx, "res-1")
	if err != nil {
		t.Fatal(err)
	}
	if g1.HolderEpoch == g2.HolderEpoch {
		t.Error("epochs should differ across re-contends")
	}
}
