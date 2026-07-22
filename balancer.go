package lease

import (
	"context"
	"sync"
	"time"
)

// TaskHandler is the interface for task execution callbacks.
// Implementations must handle Start/Stop quickly; long-running work should
// happen in background goroutines owned by the handler.
type TaskHandler interface {
	// OnStart is called when this instance starts holding the lease for taskID.
	// It must return quickly. The grant can be used for protected writes.
	OnStart(taskID string, grant Grant)

	// OnStop is called when this instance stops holding the lease for taskID.
	// It must return quickly. After OnStop returns, the task must no longer
	// perform any protected writes.
	OnStop(taskID string)
}

// TaskFunc is a simple function-based TaskHandler.
// Start and stop are separate functions; both may be nil.
type TaskFunc struct {
	Start func(taskID string, grant Grant)
	Stop  func(taskID string)
}

// OnStart calls Start if set.
func (f TaskFunc) OnStart(taskID string, grant Grant) {
	if f.Start != nil {
		f.Start(taskID, grant)
	}
}

// OnStop calls Stop if set.
func (f TaskFunc) OnStop(taskID string) {
	if f.Stop != nil {
		f.Stop(taskID)
	}
}

// MembershipProvider returns the current list of live instance IDs.
// The returned list should be sorted for deterministic assignment.
type MembershipProvider func(ctx context.Context) ([]string, error)

// TaskSetProvider returns the current set of task IDs to balance.
type TaskSetProvider func(ctx context.Context) ([]string, error)

// BalancerConfig configures an InstanceAgent.
type BalancerConfig struct {
	// Lease is the lease manager used for contend/renew/release.
	Lease Lease

	// Tasks provides the current task set.
	Tasks TaskSetProvider

	// Members provides the current instance membership.
	// If nil, the agent runs in "single instance" mode and tries to hold all tasks.
	Members MembershipProvider

	// Handler receives task start/stop events.
	Handler TaskHandler

	// RenewInterval is how often we renew held leases. Defaults to TTL/3.
	RenewInterval time.Duration

	// RebalanceInterval is how often we check for rebalance opportunities.
	// Defaults to 5s.
	RebalanceInterval time.Duration

	// TTL is the lease TTL. Used to derive default RenewInterval.
	TTL time.Duration
}

// InstanceAgent is the per-instance agent that balances tasks across members.
// It periodically:
//   - reads task set + membership
//   - computes target owners via HRW
//   - contends for tasks that should be ours
//   - releases tasks that should not be ours
//   - renews tasks we hold
type InstanceAgent struct {
	cfg      BalancerConfig
	holderID string

	mu    sync.Mutex
	holds map[string]Grant // taskID -> grant

	stopCh chan struct{}
	doneCh chan struct{}
}

// NewInstanceAgent creates an InstanceAgent.
func NewInstanceAgent(cfg BalancerConfig) *InstanceAgent {
	if cfg.TTL == 0 {
		cfg.TTL = 10 * time.Second
	}
	if cfg.RenewInterval == 0 {
		cfg.RenewInterval = cfg.TTL / 3
	}
	if cfg.RebalanceInterval == 0 {
		cfg.RebalanceInterval = 5 * time.Second
	}
	return &InstanceAgent{
		cfg:   cfg,
		holds: make(map[string]Grant),
	}
}

// Start begins the agent's rebalance and renew loops.
// It blocks until the agent is stopped.
func (a *InstanceAgent) Start(ctx context.Context) error {
	a.stopCh = make(chan struct{})
	a.doneCh = make(chan struct{})
	defer close(a.doneCh)

	rebalanceTicker := time.NewTicker(a.cfg.RebalanceInterval)
	defer rebalanceTicker.Stop()

	renewTicker := time.NewTicker(a.cfg.RenewInterval)
	defer renewTicker.Stop()

	// Run immediately on start
	if err := a.rebalance(ctx); err != nil {
		// Log-level: continue running even if first cycle fails
	}
	if err := a.renewAll(ctx); err != nil {
		// Continue
	}

	for {
		select {
		case <-ctx.Done():
			a.releaseAll(context.Background())
			return ctx.Err()
		case <-a.stopCh:
			a.releaseAll(context.Background())
			return nil
		case <-rebalanceTicker.C:
			_ = a.rebalance(ctx)
		case <-renewTicker.C:
			_ = a.renewAll(ctx)
		}
	}
}

// Stop stops the agent and releases all held leases.
func (a *InstanceAgent) Stop() {
	close(a.stopCh)
	<-a.doneCh
}

// HeldGrants returns a copy of currently held grants.
func (a *InstanceAgent) HeldGrants() map[string]Grant {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make(map[string]Grant, len(a.holds))
	for k, v := range a.holds {
		out[k] = v
	}
	return out
}

func (a *InstanceAgent) rebalance(ctx context.Context) error {
	tasks, err := a.cfg.Tasks(ctx)
	if err != nil {
		return err
	}

	members, err := a.resolveMembers(ctx)
	if err != nil {
		return err
	}

	if len(members) == 0 {
		return nil
	}

	self := a.holderIDFromManager()
	members = SortMembers(members)

	a.mu.Lock()
	holds := make(map[string]Grant, len(a.holds))
	for k, v := range a.holds {
		holds[k] = v
	}
	a.mu.Unlock()

	for _, taskID := range tasks {
		target := HRWAssign(taskID, members)
		_, held := holds[taskID]

		if target == self {
			if !held {
				grant, err := a.cfg.Lease.Contend(ctx, taskID)
				if err == nil {
					a.addHold(taskID, grant)
					if a.cfg.Handler != nil {
						a.cfg.Handler.OnStart(taskID, grant)
					}
				}
			}
			// If already held, renew handles it on its own schedule
		} else {
			if held {
				grant := holds[taskID]
				if a.cfg.Handler != nil {
					a.cfg.Handler.OnStop(taskID)
				}
				_ = a.cfg.Lease.Release(ctx, grant)
				a.removeHold(taskID)
			}
		}
	}

	// Handle tasks we hold but are no longer in the task set
	taskSet := make(map[string]struct{}, len(tasks))
	for _, t := range tasks {
		taskSet[t] = struct{}{}
	}
	for taskID, grant := range holds {
		if _, ok := taskSet[taskID]; !ok {
			if a.cfg.Handler != nil {
				a.cfg.Handler.OnStop(taskID)
			}
			_ = a.cfg.Lease.Release(ctx, grant)
			a.removeHold(taskID)
		}
	}

	return nil
}

func (a *InstanceAgent) renewAll(ctx context.Context) error {
	a.mu.Lock()
	holds := make(map[string]Grant, len(a.holds))
	for k, v := range a.holds {
		holds[k] = v
	}
	a.mu.Unlock()

	for taskID, grant := range holds {
		newGrant, err := a.cfg.Lease.Renew(ctx, grant)
		if err != nil {
			// Lost the lease
			if a.cfg.Handler != nil {
				a.cfg.Handler.OnStop(taskID)
			}
			a.removeHold(taskID)
			continue
		}
		a.updateHold(taskID, newGrant)
	}
	return nil
}

func (a *InstanceAgent) releaseAll(ctx context.Context) {
	a.mu.Lock()
	holds := make(map[string]Grant, len(a.holds))
	for k, v := range a.holds {
		holds[k] = v
	}
	a.mu.Unlock()

	for taskID, grant := range holds {
		if a.cfg.Handler != nil {
			a.cfg.Handler.OnStop(taskID)
		}
		_ = a.cfg.Lease.Release(ctx, grant)
		a.removeHold(taskID)
	}
}

func (a *InstanceAgent) resolveMembers(ctx context.Context) ([]string, error) {
	if a.cfg.Members == nil {
		// Single-instance mode: only self
		return []string{a.holderIDFromManager()}, nil
	}
	return a.cfg.Members(ctx)
}

func (a *InstanceAgent) holderIDFromManager() string {
	if a.holderID != "" {
		return a.holderID
	}
	if lm, ok := a.cfg.Lease.(*leaseImpl); ok {
		a.holderID = lm.HolderID()
	}
	return a.holderID
}

func (a *InstanceAgent) addHold(taskID string, grant Grant) {
	a.mu.Lock()
	a.holds[taskID] = grant
	a.mu.Unlock()
}

func (a *InstanceAgent) updateHold(taskID string, grant Grant) {
	a.mu.Lock()
	a.holds[taskID] = grant
	a.mu.Unlock()
}

func (a *InstanceAgent) removeHold(taskID string) {
	a.mu.Lock()
	delete(a.holds, taskID)
	a.mu.Unlock()
}
