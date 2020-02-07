package lock

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/olivere/elastic"
)

// Lock implements a distributed lock using Elasticsearch.
// The use case of this lock is improving efficiency (not correctness)
type Lock struct {
	client          *elastic.Client
	indexName       string
	typeName        string
	lastTTL         time.Duration
	ID              string    `json:"-"`
	Owner           string    `json:"owner"`
	Acquired        time.Time `json:"acquired"`
	Expires         time.Time `json:"expires"`
	isAcquired      bool
	isReleased      bool
	keepAliveActive bool
}

var (
	clientID = uuid.New().String()
)

// NewLock create a new lock identified by a string
func NewLock(client *elastic.Client, id string) *Lock {
	return &Lock{
		client:          client,
		ID:              id,
		Owner:           clientID,
		indexName:       "distributed-locks",
		typeName:        "lock",
		isAcquired:      false,
		isReleased:      false,
		keepAliveActive: false,
	}
}

// WithOwner is a shortcut method to set the owner manually.
// If you don't specify an owner, a random UUID is used automayically.
func (lock *Lock) WithOwner(owner string) *Lock {
	lock.Owner = owner
	return lock
}

// Acquire tries to acquire a lock with a TTL in seconds.
// Returns nil when succesful or error otherwise.
func (lock *Lock) Acquire(ctx context.Context, ttl time.Duration) error {
	lock.lastTTL = ttl
	lock.Acquired = time.Now()
	lock.Expires = lock.Acquired.Add(ttl)
	// This script ensures that the owner is the same so that a single process can renew a named lock over again.
	// In case the lock is expired, another process can take over.
	script := elastic.NewScript(`
	if (ctx._source.owner != params.owner && ZonedDateTime.parse(ctx._source.expires).isAfter(ZonedDateTime.parse(params.now))) {
		ctx.op = "none";
	} else {
		ctx._source.expires = params.expires;
		if (ctx._source.owner != params.owner) {
			ctx._source.owner = params.owner;
			ctx._source.acquired = params.acquired;
		}
	}
	`)
	script.Params(map[string]interface{}{
		"now":      time.Now(),
		"owner":    lock.Owner,
		"expires":  lock.Expires,
		"acquired": lock.Acquired,
	})
	resp, err := lock.client.Update().Index(lock.indexName).Type(lock.typeName).Id(lock.ID).Script(script).Upsert(lock).Refresh("true").ScriptedUpsert(true).Do(ctx)
	if elastic.IsConflict(err) || err == nil && resp.Result == "noop" {
		return errors.New("lock held by other client")
	}
	if err != nil {
		return err
	}
	lock.isAcquired = true
	return nil
}

// KeepAlive causes the lock to automatically extend its TTL to avoid expiration.
// This keep going until the context is cancelled, Release() is called, or the process dies.
// This calls Acquire again {beforeExpiry} seconds before expirt.
// Don't use KeepAlive with very short TTLs.
func (lock *Lock) KeepAlive(ctx context.Context, beforeExpiry time.Duration) error {
	if !lock.isAcquired {
		return errors.New("acquire lock before keep alive")
	}
	if lock.keepAliveActive {
		return nil
	}
	if beforeExpiry >= lock.lastTTL {
		return fmt.Errorf("KeepAlive's beforeExpire (%v) should be smaller than lock's TTL (%v)", beforeExpiry, lock.lastTTL)
	}

	// Call Acquire {beforeExpiry} seconds before lock expires
	timeLeft := lock.Expires.Add(-beforeExpiry).Sub(time.Now())
	if timeLeft <= 0 {
		timeLeft = 1 * time.Millisecond
	}
	lock.keepAliveActive = true
	time.AfterFunc(timeLeft, func() {
		lock.keepAliveActive = false
		if !lock.isReleased {
			lock.Acquire(ctx, lock.lastTTL)
			lock.KeepAlive(ctx, beforeExpiry)
		}
	})
	return nil
}

// Release removes the lock (if it is still held)
func (lock *Lock) Release() error {
	if lock.isReleased {
		return nil
	}
	ctx := context.Background()
	// Query checking that lock is still held by this client
	query := elastic.NewBoolQuery().Must(
		elastic.NewTermQuery("_id", lock.ID),
		elastic.NewTermQuery("owner", lock.Owner),
	)
	_, err := lock.client.DeleteByQuery().Index(lock.indexName).Query(query).Conflicts("proceed").Do(ctx)
	if err != nil && elastic.IsNotFound(err) == false {
		return err
	}
	lock.isReleased = true
	lock.isAcquired = false
	return nil
}

// IsAcquired returns if lock is acquired and not expired
func (lock *Lock) IsAcquired() bool {
	return lock.isAcquired && lock.Expires.After(time.Now())
}

// IsReleased returns if lock was released manually or is expired
func (lock *Lock) IsReleased() bool {
	return lock.isReleased || lock.Expires.Before(time.Now())
}
