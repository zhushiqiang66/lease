package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/a8m/lease"
)

// Mongo implements lease.Store backed by MongoDB single-document CAS.
// The resource_id is stored as _id (MongoDB's native primary key).
type Mongo struct {
	coll *mongo.Collection
}

// NewMongo creates a Mongo store using the given collection.
func NewMongo(coll *mongo.Collection) *Mongo {
	return &Mongo{coll: coll}
}

// Insert inserts a new lease only if no active lease exists for the resource.
func (s *Mongo) Insert(ctx context.Context, rec lease.Resource) (lease.Resource, error) {
	now := time.Now()

	filter := bson.M{
		"_id": rec.ID,
		"$or": []bson.M{
			{"holder_epoch": 0},
			{"expires_at": bson.M{"$lte": now}},
		},
	}

	update := bson.M{
		"$set": bson.M{
			"holder_id":    rec.HolderID,
			"holder_epoch": rec.HolderEpoch,
			"expires_at":   rec.ExpiresAt,
		},
		"$inc": bson.M{"version": 1},
	}

	opts := options.FindOneAndUpdate().
		SetUpsert(true).
		SetReturnDocument(options.After)

	var result lease.Resource
	err := s.coll.FindOneAndUpdate(ctx, filter, update, opts).Decode(&result)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return lease.Resource{}, lease.ErrLeaseHeld
		}
		var we mongo.WriteException
		if errors.As(err, &we) {
			for _, e := range we.WriteErrors {
				if e.Code == 11000 {
					return lease.Resource{}, lease.ErrLeaseHeld
				}
			}
		}
		var cfe mongo.CommandError
		if errors.As(err, &cfe) && cfe.Code == 11000 {
			return lease.Resource{}, lease.ErrLeaseHeld
		}
		return lease.Resource{}, fmt.Errorf("insert: %w", err)
	}
	return result, nil
}

// Update extends the lease if holder_epoch matches.
func (s *Mongo) Update(ctx context.Context, rec lease.Resource) (lease.Resource, error) {
	filter := bson.M{
		"_id":          rec.ID,
		"holder_epoch": rec.HolderEpoch,
	}

	update := bson.M{
		"$set": bson.M{
			"expires_at": rec.ExpiresAt,
			"holder_id":  rec.HolderID,
		},
		"$inc": bson.M{"version": 1},
	}

	opts := options.FindOneAndUpdate().
		SetReturnDocument(options.After)

	var result lease.Resource
	err := s.coll.FindOneAndUpdate(ctx, filter, update, opts).Decode(&result)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return lease.Resource{}, lease.ErrEpochMismatch
		}
		return lease.Resource{}, fmt.Errorf("update: %w", err)
	}
	return result, nil
}

// Delete clears holder fields if holder_epoch matches (soft delete).
// Idempotent: a stale epoch does not produce an error.
func (s *Mongo) Delete(ctx context.Context, resourceID string, holderEpoch int64) error {
	filter := bson.M{
		"_id":          resourceID,
		"holder_epoch": holderEpoch,
	}

	update := bson.M{
		"$set": bson.M{
			"holder_id":    "",
			"holder_epoch": 0,
			"expires_at":   time.Time{},
		},
		"$inc": bson.M{"version": 1},
	}

	result := s.coll.FindOneAndUpdate(ctx, filter, update, options.FindOneAndUpdate())
	err := result.Err()
	if err != nil && errors.Is(err, mongo.ErrNoDocuments) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	return nil
}

// Get reads the current record for a resource.
func (s *Mongo) Get(ctx context.Context, resourceID string) (lease.Resource, error) {
	var rec lease.Resource
	err := s.coll.FindOne(ctx, bson.M{"_id": resourceID}).Decode(&rec)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return lease.Resource{}, lease.ErrLeaseNotFound
		}
		return lease.Resource{}, fmt.Errorf("get: %w", err)
	}
	return rec, nil
}
