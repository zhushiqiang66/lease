// Simple worker example: one instance holds leases and processes tasks.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
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

	mgr := lease.New(s,
		lease.WithTTL(15*time.Second),
		lease.WithHolderID("worker-"+fmt.Sprintf("%d", os.Getpid())),
	)

	// Static task set
	tasks := []string{"task-alpha", "task-beta", "task-gamma", "task-delta"}

	holderID := "worker-" + fmt.Sprintf("%d", os.Getpid())
	runner := &taskRunner{}

	b := lease.NewBalancer(holderID, mgr,
		func(ctx context.Context) ([]string, error) { return tasks, nil },
		nil,
		runner,
		lease.WithBalancerTTL(15*time.Second),
		lease.WithRebalanceInterval(5*time.Second),
	)

	b.Start()

	// Wait for signal
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("shutting down...")
	b.Stop()
	log.Println("done")
}

type taskRunner struct{}

func (r *taskRunner) OnStart(taskID string, grant lease.Grant) {
	log.Printf("[START] task=%s epoch=%d", taskID, grant.HolderEpoch)
	go runTask(taskID, grant)
}

func (r *taskRunner) OnStop(taskID string) {
	log.Printf("[STOP]  task=%s", taskID)
}

func runTask(taskID string, grant lease.Grant) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		log.Printf("[WORK]  task=%s epoch=%d", taskID, grant.HolderEpoch)
	}
}
