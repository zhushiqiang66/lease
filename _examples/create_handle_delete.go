// Create, handle, and delete leases.
// Demonstrates the basic lifecycle: create a task, hold its lease, do work, delete when done.
package main

import (
	"context"
	"fmt"
	"log"
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

	mgr := lease.New(store,
		lease.WithTTL(15*time.Second),
		lease.WithHolderID("worker-create"),
	)

	// Create a few tasks by contending for them
	taskIDs := []string{"task-100", "task-101", "task-102"}
	for _, id := range taskIDs {
		grant, err := mgr.Contend(context.Background(), id)
		if err != nil {
			log.Printf("contend %s: %v", id, err)
			continue
		}
		log.Printf("acquired %s epoch=%d expires=%s", id, grant.HolderEpoch, grant.ExpiresAt.Format(time.RFC3339))
	}

	// Observe one
	g, err := mgr.Observe(context.Background(), "task-100")
	if err != nil {
		log.Fatalf("observe: %v", err)
	}
	log.Printf("observe task-100: holder=%s epoch=%d", g.HolderID, g.HolderEpoch)

	// Simulate handling: renew loop
	fmt.Println("\n--- handling with renew loop (5s) ---")
	deadline := time.After(5 * time.Second)
	tick := time.NewTicker(3 * time.Second)
	defer tick.Stop()

	held, _ := mgr.Observe(context.Background(), "task-100")
	for {
		select {
		case <-deadline:
			fmt.Println("done handling")
			// Release
			if err := mgr.Release(context.Background(), held); err != nil {
				log.Printf("release: %v", err)
			}
			log.Println("released task-100")
			return
		case <-tick.C:
			renewed, err := mgr.Renew(context.Background(), held)
			if err != nil {
				log.Printf("renew failed: %v", err)
				return
			}
			held = renewed
			log.Printf("renewed task-100 epoch=%d expires=%s", held.HolderEpoch, held.ExpiresAt.Format(time.RFC3339))
		}
	}
}
