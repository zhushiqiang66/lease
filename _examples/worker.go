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

	handler := lease.TaskFunc{
		Start: func(taskID string, grant lease.Grant) {
			log.Printf("[START] task=%s epoch=%d", taskID, grant.HolderEpoch)
			go runTask(taskID, grant)
		},
		Stop: func(taskID string) {
			log.Printf("[STOP]  task=%s", taskID)
		},
	}

	agent := lease.NewInstanceAgent(lease.BalancerConfig{
		Lease: mgr,
		Tasks: func(ctx context.Context) ([]string, error) {
			return tasks, nil
		},
		Handler:           handler,
		TTL:               15 * time.Second,
		RebalanceInterval: 5 * time.Second,
	})

	go agent.Start(context.Background())

	// Wait for signal
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("shutting down...")
	agent.Stop()
	log.Println("done")
}

func runTask(taskID string, grant lease.Grant) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		log.Printf("[WORK]  task=%s epoch=%d", taskID, grant.HolderEpoch)
	}
}
