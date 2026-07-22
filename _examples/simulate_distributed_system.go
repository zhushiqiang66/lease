// Simulate a distributed system: two instances with HRW-based task balancing.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/a8m/lease"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatalf("connect mongo: %v", err)
	}
	defer client.Disconnect(ctx)

	coll := client.Database("lease_demo").Collection("leases")

	store := lease.NewMongoStore(coll)

	// Two instances sharing the same members list
	members := []string{"instance-A", "instance-B"}

	tasks := make([]string, 10)
	for i := range tasks {
		tasks[i] = fmt.Sprintf("task-%02d", i)
	}

	makeAgent := func(id string) *lease.InstanceAgent {
		store := lease.NewMongoStore(coll)
		mgr := lease.New(store,
			lease.WithTTL(10*time.Second),
			lease.WithHolderID(id),
		)
		handler := lease.TaskFunc{
			Start: func(taskID string, grant lease.Grant) {
				log.Printf("[%s] START %s (epoch=%d)", id, taskID, grant.HolderEpoch)
			},
			Stop: func(taskID string) {
				log.Printf("[%s] STOP  %s", id, taskID)
			},
		}
		return lease.NewInstanceAgent(lease.BalancerConfig{
			Lease: mgr,
			Tasks: func(ctx context.Context) ([]string, error) {
				return tasks, nil
			},
			Members: func(ctx context.Context) ([]string, error) {
				return members, nil
			},
			Handler:           handler,
			TTL:               10 * time.Second,
			RebalanceInterval: 2 * time.Second,
			RenewInterval:     3 * time.Second,
		})
	}

	agentA := makeAgent("instance-A")
	agentB := makeAgent("instance-B")

	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithCancel(context.Background())

	go agentA.Start(ctxA)
	go agentB.Start(ctxB)

	// Run for 15 seconds, then stop A and watch B take over
	log.Println("=== both instances running ===")
	time.Sleep(15 * time.Second)

	log.Printf("=== held by A: %d, held by B: %d ===",
		len(agentA.HeldGrants()), len(agentB.HeldGrants()))

	log.Println("=== stopping instance A ===")
	cancelA()
	time.Sleep(12 * time.Second)

	log.Printf("=== after A stopped: held by B: %d ===", len(agentB.HeldGrants()))

	cancelB()
	time.Sleep(time.Second)
	log.Println("done")
	os.Exit(0)
}
