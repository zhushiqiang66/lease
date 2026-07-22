package lease

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoStore implements Store backed by MongoDB single-document CAS.
// The resource_id is stored as _id (MongoDB's native primary key).
type MongoStore struct {
	coll  *mongo.Collection
	clock Clock
}

// MongoStoreOption configures a MongoStore.
type MongoStoreOption func(*MongoStore)

// WithMongoClock sets the clock. Defaults to real time.
func WithMongoClock(c Clock) MongoStoreOption {
	return func(s *MongoStore) { s.clock = c }
}

// NewMongoStore creates a MongoStore using the given collection.
func NewMongoStore(coll *mongo.Collection, opts ...MongoStoreOption) *MongoStore {
	s := &MongoStore{
		coll:  coll,
		clock: realClock{},
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Insert inserts a new lease only if no active lease exists for the resource.
// Uses _id as the primary key with upsert; condition is expired or free.
func (s *MongoStore) Insert(ctx context.Context, rec Record) (Record, error) {
	now := s.clock.Now()

	filter := bson.M{
		"_id": rec.ResourceID,
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

	var result Record
	err := s.coll.FindOneAndUpdate(ctx, filter, update, opts).Decode(&result)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return Record{}, ErrLeaseHeld
		}
		// Duplicate key means the doc exists but doesn't match our filter (someone else holds it)
		var we mongo.WriteException
		if errors.As(err, &we) {
			for _, e := range we.WriteErrors {
				if e.Code == 11000 {
					return Record{}, ErrLeaseHeld
				}
			}
		}
		var cfe mongo.CommandError
		if errors.As(err, &cfe) && cfe.Code == 11000 {
			return Record{}, ErrLeaseHeld
		}
		return Record{}, fmt.Errorf("insert: %w", err)
	}
	return result, nil
}

// Update extends the lease if holder_epoch matches.
func (s *MongoStore) Update(ctx context.Context, rec Record) (Record, error) {
	filter := bson.M{
		"_id":          rec.ResourceID,
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

	var result Record
	err := s.coll.FindOneAndUpdate(ctx, filter, update, opts).Decode(&result)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return Record{}, ErrEpochMismatch
		}
		return Record{}, fmt.Errorf("update: %w", err)
	}
	return result, nil
}

// Delete clears holder fields if holder_epoch matches (soft delete).
// Idempotent: a stale epoch does not produce an error.
func (s *MongoStore) Delete(ctx context.Context, resourceID string, holderEpoch int64) error {
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
func (s *MongoStore) Get(ctx context.Context, resourceID string) (Record, error) {
	var rec Record
	err := s.coll.FindOne(ctx, bson.M{"_id": resourceID}).Decode(&rec)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return Record{}, ErrLeaseNotFound
		}
		return Record{}, fmt.Errorf("get: %w", err)
	}
	return rec, nil
}
