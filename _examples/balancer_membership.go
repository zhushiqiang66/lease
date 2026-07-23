// Demonstrate Balancer rebalancing when membership AND task set change.
//
//  Phase 1: 4 tasks, 2 members (A, B)
//  Phase 2: 4 tasks, 3 members (A, B, C)  — add member
//  Phase 3: 8 tasks, 3 members            — grow tasks
//  Phase 4: 2 tasks, 3 members            — shrink tasks
//
// Each member runs its own Balancer against a shared lease store.
package main

import (
	"context"
	"log"
	"sort"
	"time"

	"github.com/a8m/lease"
	"github.com/a8m/lease/store"
)

type demoRunner struct {
	id string
}

func (r *demoRunner) OnStart(taskID string, grant lease.Grant) {
	log.Printf("[%s] START %s (epoch=%d)", r.id, taskID, grant.HolderEpoch)
}

func (r *demoRunner) OnStop(taskID string) {
	log.Printf("[%s] STOP  %s", r.id, taskID)
}

type node struct {
	id  string
	bal *lease.Balancer
}

func newNode(id string, s lease.Store,
	getTasks func(context.Context) ([]string, error),
	getMembers func(context.Context) ([]string, error),
) *node {
	mgr := lease.New(s,
		lease.WithTTL(5*time.Second),
		lease.WithHolderID(id),
	)
	bal := lease.NewBalancer(id, mgr,
		getTasks,
		getMembers,
		&demoRunner{id: id},
		lease.WithBalancerTTL(5*time.Second),
		lease.WithRebalanceInterval(100*time.Millisecond),
		lease.WithRenewInterval(500*time.Millisecond),
	)
	return &node{id: id, bal: bal}
}

func (n *node) start() { n.bal.Start() }
func (n *node) stop()  { n.bal.Stop() }

func main() {
	s := store.NewMemory()

	var currentTasks []string
	var currentMembers []string
	getTasks := func(ctx context.Context) ([]string, error) { return currentTasks, nil }
	getMembers := func(ctx context.Context) ([]string, error) { return currentMembers, nil }

	taskIDs := func(m map[string]lease.Grant) []string {
		ids := make([]string, 0, len(m))
		for k := range m {
			ids = append(ids, k)
		}
		sort.Strings(ids)
		return ids
	}

	dump := func(phase string, nodes ...*node) {
		log.Printf("=== %s ===", phase)
		for _, n := range nodes {
			log.Printf("  %s holds %d tasks: %v", n.id, len(n.bal.HeldGrants()), taskIDs(n.bal.HeldGrants()))
		}
	}

	// --- Phase 1: 4 tasks, 2 members ---
	log.Println("=== Phase 1: 4 tasks, 2 members (A, B) ===")
	currentTasks = []string{"task-1", "task-2", "task-3", "task-4"}
	currentMembers = []string{"node-A", "node-B"}
	nodeA := newNode("node-A", s, getTasks, getMembers)
	nodeB := newNode("node-B", s, getTasks, getMembers)
	nodeA.start()
	nodeB.start()

	time.Sleep(500 * time.Millisecond)
	dump("Phase 1", nodeA, nodeB)

	// --- Phase 2: add member C (3 total), still 4 tasks ---
	log.Println("=== Phase 2: 4 tasks, 3 members (A, B, C) ===")
	currentMembers = []string{"node-A", "node-B", "node-C"}
	nodeC := newNode("node-C", s, getTasks, getMembers)
	nodeC.start()

	time.Sleep(500 * time.Millisecond)
	dump("Phase 2", nodeA, nodeB, nodeC)

	// --- Phase 3: grow to 8 tasks, 3 members ---
	log.Println("=== Phase 3: 8 tasks, 3 members ===")
	currentTasks = []string{"task-1", "task-2", "task-3", "task-4", "task-5", "task-6", "task-7", "task-8"}

	// Wait for the balancer to pick up all 8 tasks
	for i := 0; i < 20; i++ {
		total := len(nodeA.bal.HeldGrants()) + len(nodeB.bal.HeldGrants()) + len(nodeC.bal.HeldGrants())
		if total == len(currentTasks) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	dump("Phase 3", nodeA, nodeB, nodeC)

	// --- Phase 4: shrink to 2 tasks, 3 members ---
	log.Println("=== Phase 4: 2 tasks, 3 members ===")
	currentTasks = []string{"task-1", "task-2"}

	time.Sleep(500 * time.Millisecond)
	dump("Phase 4", nodeA, nodeB, nodeC)

	nodeA.stop()
	nodeB.stop()
	nodeC.stop()
	log.Println("done")
}
