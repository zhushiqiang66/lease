// Example showing how to attach metadata to a lease record via the store directly.
//
// In the generic lease design, metadata belongs to the Record, not the Grant.
// For protected writes, always carry holder_epoch in your write condition.
package main

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
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
		lease.WithHolderID("worker-meta"),
	)

	resourceID := "task-with-metadata"

	// 1. Contend for the lease
	grant, err := mgr.Contend(context.Background(), resourceID)
	if err != nil {
		log.Fatalf("contend: %v", err)
	}
	log.Printf("acquired %s epoch=%d", resourceID, grant.HolderEpoch)

	// 2. Protected write: update metadata only if holder_epoch still matches
	//    This is how you do safe business-state updates under lease protection.
	result := coll.FindOneAndUpdate(
		context.Background(),
		bson.M{
			"_id":          resourceID,
			"holder_epoch": grant.HolderEpoch,
		},
		bson.M{
			"$set": bson.M{
				"metadata.status":      "processing",
				"metadata.last_update": time.Now().Unix(),
				"metadata.results":     []string{"200", "500", "404"},
			},
		},
		options.FindOneAndUpdate().SetReturnDocument(options.After),
	)
	var updated lease.Record
	if err := result.Decode(&updated); err != nil {
		log.Fatalf("protected update failed (maybe lost lease): %v", err)
	}
	log.Printf("protected write succeeded: status=%s, version=%d",
		updated.Metadata["status"], updated.Version)

	// 3. Renew and do more work
	renewed, err := mgr.Renew(context.Background(), grant)
	if err != nil {
		log.Fatalf("renew: %v", err)
	}
	log.Printf("renewed: epoch=%d", renewed.HolderEpoch)

	// 4. Release
	if err := mgr.Release(context.Background(), renewed); err != nil {
		log.Printf("release: %v", err)
	}
	log.Println("released")
}
