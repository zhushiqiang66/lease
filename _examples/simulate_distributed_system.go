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
	"github.com/a8m/lease/store"
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

	s := store.NewMongo(coll)

	// Two instances sharing the same members list
	members := []string{"instance-A", "instance-B"}

	tasks := make([]string, 10)
	for i := range tasks {
		tasks[i] = fmt.Sprintf("task-%02d", i)
	}

	makeBalancer := func(id string) *lease.Balancer {
		mgr := lease.New(s,
			lease.WithTTL(10*time.Second),
			lease.WithHolderID(id),
		)
		runner := &distRunner{id: id}
		return lease.NewBalancer(id, mgr,
			func(ctx context.Context) ([]string, error) { return tasks, nil },
			func(ctx context.Context) ([]string, error) { return members, nil },
			runner,
			lease.WithBalancerTTL(10*time.Second),
			lease.WithRebalanceInterval(2*time.Second),
			lease.WithRenewInterval(3*time.Second),
		)
	}

	balancerA := makeBalancer("instance-A")
	balancerB := makeBalancer("instance-B")

	balancerA.Start()
	balancerB.Start()

	// Run for 15 seconds, then stop A and watch B take over
	log.Println("=== both instances running ===")
	time.Sleep(15 * time.Second)

	log.Printf("=== held by A: %d, held by B: %d ===",
		len(balancerA.HeldGrants()), len(balancerB.HeldGrants()))

	log.Println("=== stopping instance A ===")
	balancerA.Stop()
	time.Sleep(12 * time.Second)

	log.Printf("=== after A stopped: held by B: %d ===", len(balancerB.HeldGrants()))

	balancerB.Stop()
	time.Sleep(time.Second)
	log.Println("done")
	os.Exit(0)
}

type distRunner struct {
	id string
}

func (r *distRunner) OnStart(taskID string, grant lease.Grant) {
	log.Printf("[%s] START %s (epoch=%d)", r.id, taskID, grant.HolderEpoch)
}

func (r *distRunner) OnStop(taskID string) {
	log.Printf("[%s] STOP  %s", r.id, taskID)
}
