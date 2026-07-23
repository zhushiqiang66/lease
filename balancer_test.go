package lease_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a8m/lease"
	"github.com/a8m/lease/store"
)

type testRunner struct {
	onStart func(taskID string, grant lease.Grant)
	onStop  func(taskID string)
}

func (r testRunner) OnStart(taskID string, grant lease.Grant) {
	if r.onStart != nil {
		r.onStart(taskID, grant)
	}
}

func (r testRunner) OnStop(taskID string) {
	if r.onStop != nil {
		r.onStop(taskID)
	}
}

func TestBalancerSingleInstance(t *testing.T) {
	s := store.NewMemory()
	m := lease.New(s, lease.WithHolderID("worker-1"), lease.WithTTL(30*time.Second))

	tasks := []string{"task-1", "task-2", "task-3"}

	var started int32
	runner := testRunner{
		onStart: func(taskID string, grant lease.Grant) {
			atomic.AddInt32(&started, 1)
		},
	}

	b := lease.NewBalancer("worker-1", m,
		func(ctx context.Context) ([]string, error) { return tasks, nil },
		nil,
		runner,
		lease.WithBalancerTTL(30*time.Second),
		lease.WithRebalanceInterval(10*time.Millisecond),
		lease.WithRenewInterval(100*time.Millisecond),
	)

	go b.Start()
	defer b.Stop()

	time.Sleep(200 * time.Millisecond)

	held := b.HeldGrants()
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

	var started1 int32
	runner1 := testRunner{
		onStart: func(taskID string, grant lease.Grant) { atomic.AddInt32(&started1, 1) },
	}
	var started2 int32
	runner2 := testRunner{
		onStart: func(taskID string, grant lease.Grant) { atomic.AddInt32(&started2, 1) },
	}

	b1 := lease.NewBalancer("worker-1", m1,
		func(ctx context.Context) ([]string, error) { return tasks, nil },
		func(ctx context.Context) ([]string, error) { return members, nil },
		runner1,
		lease.WithBalancerTTL(30*time.Second),
		lease.WithRebalanceInterval(10*time.Millisecond),
		lease.WithRenewInterval(100*time.Millisecond),
	)
	b2 := lease.NewBalancer("worker-2", m2,
		func(ctx context.Context) ([]string, error) { return tasks, nil },
		func(ctx context.Context) ([]string, error) { return members, nil },
		runner2,
		lease.WithBalancerTTL(30*time.Second),
		lease.WithRebalanceInterval(10*time.Millisecond),
		lease.WithRenewInterval(100*time.Millisecond),
	)

	go b1.Start()
	go b2.Start()
	defer b1.Stop()
	defer b2.Stop()

	time.Sleep(300 * time.Millisecond)

	held1 := b1.HeldGrants()
	held2 := b2.HeldGrants()

	total := len(held1) + len(held2)
	if total != len(tasks) {
		t.Errorf("total held = %d, want %d (held1=%d held2=%d)", total, len(tasks), len(held1), len(held2))
	}

	for k := range held1 {
		if _, ok := held2[k]; ok {
			t.Errorf("task %s held by both", k)
		}
	}

	diff := abs(len(held1) - len(held2))
	if diff > 10 {
		t.Logf("warning: high imbalance: %d vs %d (diff=%d)", len(held1), len(held2), diff)
	}
}

func TestBalancerReleaseOnStop(t *testing.T) {
	s := store.NewMemory()
	m := lease.New(s, lease.WithHolderID("worker-1"), lease.WithTTL(30*time.Second))

	tasks := []string{"task-1", "task-2"}
	stopped := make(chan string, 10)
	runner := testRunner{
		onStart: func(taskID string, grant lease.Grant) {},
		onStop: func(taskID string) {
			stopped <- taskID
		},
	}

	b := lease.NewBalancer("worker-1", m,
		func(ctx context.Context) ([]string, error) { return tasks, nil },
		nil,
		runner,
		lease.WithBalancerTTL(30*time.Second),
		lease.WithRebalanceInterval(10*time.Millisecond),
		lease.WithRenewInterval(100*time.Millisecond),
	)

	go b.Start()

	time.Sleep(100 * time.Millisecond)

	b.Stop()
	time.Sleep(50 * time.Millisecond)

	if len(stopped) != 2 {
		t.Errorf("stopped %d tasks on stop, want 2", len(stopped))
	}

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

	b := lease.NewBalancer("worker-1", m,
		func(ctx context.Context) ([]string, error) { return currentTasks, nil },
		nil,
		testRunner{},
		lease.WithBalancerTTL(30*time.Second),
		lease.WithRebalanceInterval(10*time.Millisecond),
		lease.WithRenewInterval(100*time.Millisecond),
	)

	go b.Start()
	defer b.Stop()

	time.Sleep(100 * time.Millisecond)
	if got := len(b.HeldGrants()); got != 2 {
		t.Fatalf("initial: %d, want 2", got)
	}

	currentTasks = []string{"task-1", "task-2", "task-3"}
	time.Sleep(100 * time.Millisecond)
	if got := len(b.HeldGrants()); got != 3 {
		t.Errorf("after add: %d, want 3", got)
	}

	currentTasks = []string{"task-1"}
	time.Sleep(100 * time.Millisecond)
	if got := len(b.HeldGrants()); got != 1 {
		t.Errorf("after remove: %d, want 1", got)
	}
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
