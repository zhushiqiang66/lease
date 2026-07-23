package lease

import (
	"context"
	"sync"
	"time"
)

// TaskRunner controls the lifecycle of a single task instance.
//
// Implementations must be safe for concurrent calls to Start/Stop for
// different task IDs. Start and Stop for the same task will be serialized
// by the Balancer.
type TaskRunner interface {
	// OnStart is called when this instance starts holding the lease for taskID.
	// It must return quickly. The grant can be used for protected writes.
	OnStart(taskID string, grant Grant)

	// OnStop is called when this instance stops holding the lease for taskID.
	// It must return quickly. After OnStop returns, the task must no longer
	// perform any protected writes.
	OnStop(taskID string)
}

// MembershipProvider returns the current list of live instance IDs.
// The returned list should be sorted for deterministic assignment.
type MembershipProvider func(ctx context.Context) ([]string, error)

// TaskSetProvider returns the current set of task IDs to balance.
type TaskSetProvider func(ctx context.Context) ([]string, error)

// Balancer distributes tasks across instances using HRW hashing.
// It periodically:
//   - reads task set + membership
//   - computes target owners via HRW
//   - contends for tasks that should be ours
//   - releases tasks that should not be ours
//   - renews tasks we hold
type Balancer struct {
	lease   Lease
	tasks   TaskSetProvider
	members MembershipProvider
	runner  TaskRunner

	ttl               time.Duration
	renewInterval     time.Duration
	rebalanceInterval time.Duration

	owner string

	mu    sync.Mutex
	holds map[string]Grant

	stopCh chan struct{}
	doneCh chan struct{}
}

// BalancerOption configures a Balancer.
type BalancerOption func(*Balancer)

// WithBalancerTTL sets the lease TTL. Defaults to 10s.
func WithBalancerTTL(d time.Duration) BalancerOption {
	return func(b *Balancer) { b.ttl = d }
}

// WithRenewInterval sets how often held leases are renewed.
// Defaults to TTL/3.
func WithRenewInterval(d time.Duration) BalancerOption {
	return func(b *Balancer) { b.renewInterval = d }
}

// WithRebalanceInterval sets how often the balancer checks for
// rebalance opportunities. Defaults to 5s.
func WithRebalanceInterval(d time.Duration) BalancerOption {
	return func(b *Balancer) { b.rebalanceInterval = d }
}

// NewBalancer creates a new Balancer.
//
// lease, owner, tasks, members, and runner are required. All other
// settings have defaults and can be overridden with BalancerOption values.
func NewBalancer(owner string,
	lease Lease, tasks TaskSetProvider, members MembershipProvider, runner TaskRunner,
	opts ...BalancerOption) *Balancer {
	b := &Balancer{
		owner:             owner,
		lease:             lease,
		tasks:             tasks,
		members:           members,
		runner:            runner,
		ttl:               10 * time.Second,
		renewInterval:     (10 * time.Second) / 3,
		rebalanceInterval: 5 * time.Second,
		holds:             make(map[string]Grant),
	}

	for _, opt := range opts {
		opt(b)
	}

	return b
}

// Start begins the balancer's rebalance and renew loops.
// It blocks until the balancer is stopped.
func (b *Balancer) Start() error {
	ctx := context.TODO()
	b.stopCh = make(chan struct{})
	b.doneCh = make(chan struct{})
	defer close(b.doneCh)

	rebalanceTicker := time.NewTicker(b.rebalanceInterval)
	defer rebalanceTicker.Stop()

	renewTicker := time.NewTicker(b.renewInterval)
	defer renewTicker.Stop()

	if err := b.rebalance(ctx); err != nil {
	}
	if err := b.renew(ctx); err != nil {
	}

	for {
		select {
		case <-b.stopCh:
			b.release(ctx)
			return nil
		case <-rebalanceTicker.C:
			_ = b.rebalance(ctx)
		case <-renewTicker.C:
			_ = b.renew(ctx)
		}
	}
}

// Stop stops the balancer and releases all held leases.
func (b *Balancer) Stop() {
	close(b.stopCh)
	<-b.doneCh
}

// HeldGrants returns a copy of currently held grants.
func (b *Balancer) HeldGrants() map[string]Grant {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[string]Grant, len(b.holds))
	for k, v := range b.holds {
		out[k] = v
	}
	return out
}

// rebalance the task set.
// It reads the current task set + membership, computes target owners via HRW,
// contests for tasks that should be ours, releases tasks that should not be ours,
// and renews tasks we hold.
func (b *Balancer) rebalance(ctx context.Context) error {
	tasks, err := b.tasks(ctx)
	if err != nil {
		return err
	}

	members, err := b.resolveMembers(ctx)
	if err != nil {
		return err
	}

	if len(members) == 0 {
		return nil
	}

	self := b.owner
	members = CanonicalMembers(members)

	b.mu.Lock()
	holds := make(map[string]Grant, len(b.holds))
	for k, v := range b.holds {
		holds[k] = v
	}
	b.mu.Unlock()

	for _, taskID := range tasks {
		target := PickOwner(taskID, members)
		_, held := holds[taskID]

		if target == self {
			if !held {
				grant, err := b.lease.Contend(ctx, taskID)
				if err == nil {
					b.add(taskID, grant)
					b.runner.OnStart(taskID, grant)
				}
			}
		} else {
			if held {
				grant := holds[taskID]
				b.runner.OnStop(taskID)
				_ = b.lease.Release(ctx, grant)
				b.remove(taskID)
			}
		}
	}

	taskSet := make(map[string]struct{}, len(tasks))
	for _, t := range tasks {
		taskSet[t] = struct{}{}
	}
	for taskID, grant := range holds {
		if _, ok := taskSet[taskID]; !ok {
			b.runner.OnStop(taskID)
			_ = b.lease.Release(ctx, grant)
			b.remove(taskID)
		}
	}

	return nil
}

// renew renews all held leases.
func (b *Balancer) renew(ctx context.Context) error {
	b.mu.Lock()
	holds := make(map[string]Grant, len(b.holds))
	for k, v := range b.holds {
		holds[k] = v
	}
	b.mu.Unlock()

	for taskID, grant := range holds {
		newGrant, err := b.lease.Renew(ctx, grant)
		if err != nil {
			b.runner.OnStop(taskID)
			b.remove(taskID)
			continue
		}
		b.add(taskID, newGrant)
	}
	return nil
}

// release releases all held leases.
func (b *Balancer) release(ctx context.Context) {
	b.mu.Lock()
	holds := make(map[string]Grant, len(b.holds))
	for k, v := range b.holds {
		holds[k] = v
	}
	b.mu.Unlock()

	for taskID, grant := range holds {
		b.runner.OnStop(taskID)
		_ = b.lease.Release(ctx, grant)
		b.remove(taskID)
	}
}

func (b *Balancer) resolveMembers(ctx context.Context) ([]string, error) {
	if b.members == nil {
		return []string{b.owner}, nil
	}
	return b.members(ctx)
}

// add adds a grant to the balancer's map of held grants.
func (b *Balancer) add(taskID string, grant Grant) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.holds[taskID] = grant
}

// remove removes a grant from the balancer's map of held grants.
func (b *Balancer) remove(taskID string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.holds, taskID)
}
