package lease_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a8m/lease"
	"github.com/a8m/lease/store"
)

func TestBalancerSingleInstance(t *testing.T) {
	s := store.NewMemory()
	m := lease.New(s, lease.WithHolderID("worker-1"), lease.WithTTL(30*time.Second))

	tasks := []string{"task-1", "task-2", "task-3"}

	var started, stopped int32
	handler := lease.TaskFunc{
		Start: func(taskID string, grant lease.Grant) {
			atomic.AddInt32(&started, 1)
		},
		Stop: func(taskID string) {
			atomic.AddInt32(&stopped, 1)
		},
	}

	agent := lease.NewInstanceAgent(lease.BalancerConfig{
		Lease: m,
		Tasks: func(ctx context.Context) ([]string, error) {
			return tasks, nil
		},
		Handler:           handler,
		TTL:               30 * time.Second,
		RebalanceInterval: 10 * time.Millisecond,
		RenewInterval:     100 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go agent.Start(ctx)

	time.Sleep(200 * time.Millisecond)

	held := agent.HeldGrants()
	if len(held) != 3 {
		t.Errorf("held %d tasks, want 3: %v", len(held), held)
	}
	if got := atomic.LoadInt32(&started); got != 3 {
		t.Errorf("started %d, want 3", got)
	}
}

func TestBalancerTwoInstances(t *testing.T) {
	s := store.NewMemory()
	m1 := lease.New(s, lease.WithHolderID("worker-1"), lease.WithTTL(30*time.Second))
	m2 := lease.New(s, lease.WithHolderID("worker-2"), lease.WithTTL(30*time.Second))

	tasks := make([]string, 20)
	for i := range tasks {
		tasks[i] = "task-" + string(rune('a'+i%26))
	}

	members := []string{"worker-1", "worker-2"}

	var started1, stopped1 int32
	handler1 := lease.TaskFunc{
		Start: func(taskID string, grant lease.Grant) { atomic.AddInt32(&started1, 1) },
		Stop:  func(taskID string) { atomic.AddInt32(&stopped1, 1) },
	}
	var started2, stopped2 int32
	handler2 := lease.TaskFunc{
		Start: func(taskID string, grant lease.Grant) { atomic.AddInt32(&started2, 1) },
		Stop:  func(taskID string) { atomic.AddInt32(&stopped2, 1) },
	}

	agent1 := lease.NewInstanceAgent(lease.BalancerConfig{
		Lease: m1,
		Tasks: func(ctx context.Context) ([]string, error) { return tasks, nil },
		Members: func(ctx context.Context) ([]string, error) { return members, nil },
		Handler:           handler1,
		TTL:               30 * time.Second,
		RebalanceInterval: 10 * time.Millisecond,
		RenewInterval:     100 * time.Millisecond,
	})
	agent2 := lease.NewInstanceAgent(lease.BalancerConfig{
		Lease: m2,
		Tasks: func(ctx context.Context) ([]string, error) { return tasks, nil },
		Members: func(ctx context.Context) ([]string, error) { return members, nil },
		Handler:           handler2,
		TTL:               30 * time.Second,
		RebalanceInterval: 10 * time.Millisecond,
		RenewInterval:     100 * time.Millisecond,
	})

	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())

	go agent1.Start(ctx1)
	go agent2.Start(ctx2)

	time.Sleep(300 * time.Millisecond)

	held1 := agent1.HeldGrants()
	held2 := agent2.HeldGrants()

	// Total should equal total tasks
	total := len(held1) + len(held2)
	if total != len(tasks) {
		t.Errorf("total held = %d, want %d (held1=%d held2=%d", total, len(tasks), len(held1), len(held2))
	}

	// Check no overlap
	for k := range held1 {
		if _, ok := held2[k]; ok {
			t.Errorf("task %s held by both", k)
		}
	}

	// HRW distributes roughly evenly with 20 tasks
	diff := abs(len(held1) - len(held2))
	if diff > 10 {
		t.Logf("warning: high imbalance: %d vs %d (diff=%d)", len(held1), len(held2), diff)
	}

	cancel1()
	cancel2()
	time.Sleep(50 * time.Millisecond)
}

func TestBalancerReleaseOnStop(t *testing.T) {
	s := store.NewMemory()
	m := lease.New(s, lease.WithHolderID("worker-1"), lease.WithTTL(30*time.Second))

	tasks := []string{"task-1", "task-2"}
	stopped := make(chan string, 10)
	handler := lease.TaskFunc{
		Start: func(taskID string, grant lease.Grant) {},
		Stop: func(taskID string) {
			stopped <- taskID
		},
	}

	agent := lease.NewInstanceAgent(lease.BalancerConfig{
		Lease: m,
		Tasks: func(ctx context.Context) ([]string, error) {
			return tasks, nil
		},
		Handler:           handler,
		TTL:               30 * time.Second,
		RebalanceInterval: 10 * time.Millisecond,
		RenewInterval:     100 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	go agent.Start(ctx)

	time.Sleep(100 * time.Millisecond)

	// Stop via context cancel
	cancel()
	time.Sleep(50 * time.Millisecond)

	if len(stopped) != 2 {
		t.Errorf("stopped %d tasks on cancel, want 2", len(stopped))
	}

	// All leases should be released
	for _, id := range tasks {
		g, err := m.Observe(context.Background(), id)
		if err != nil && err != lease.ErrLeaseNotFound {
			t.Fatalf("observe %s: %v", id, err)
		}
		if g.HolderEpoch != 0 {
			t.Errorf("lease %s still held after stop", id)
		}
	}
}

func TestBalancerTaskSetChange(t *testing.T) {
	s := store.NewMemory()
	m := lease.New(s, lease.WithHolderID("worker-1"), lease.WithTTL(30*time.Second))

	currentTasks := []string{"task-1", "task-2"}

	agent := lease.NewInstanceAgent(lease.BalancerConfig{
		Lease: m,
		Tasks: func(ctx context.Context) ([]string, error) {
			return currentTasks, nil
		},
		TTL:               30 * time.Second,
		RebalanceInterval: 10 * time.Millisecond,
		RenewInterval:     100 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.Start(ctx)

	time.Sleep(100 * time.Millisecond)
	if got := len(agent.HeldGrants()); got != 2 {
		t.Fatalf("initial: %d, want 2", got)
	}

	// Add a new task
	currentTasks = []string{"task-1", "task-2", "task-3"}
	time.Sleep(100 * time.Millisecond)
	if got := len(agent.HeldGrants()); got != 3 {
		t.Errorf("after add: %d, want 3", got)
	}

	// Remove a task
	currentTasks = []string{"task-1"}
	time.Sleep(100 * time.Millisecond)
	if got := len(agent.HeldGrants()); got != 1 {
		t.Errorf("after remove: %d, want 1", got)
	}
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
