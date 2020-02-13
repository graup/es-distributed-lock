package lock

import (
	"context"
	"fmt"
	"sync"
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
	mutex           *sync.Mutex
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
		mutex:           &sync.Mutex{},
	}
}

// WithOwner is a shortcut method to set the owner manually.
// If you don't specify an owner, a random UUID is used automatically.
func (lock *Lock) WithOwner(owner string) *Lock {
	lock.Owner = owner
	return lock
}

// Acquire tries to acquire a lock with a TTL.
// Returns nil when succesful or error otherwise.
func (lock *Lock) Acquire(ctx context.Context, ttl time.Duration) error {
	lock.mutex.Lock()
	defer lock.mutex.Unlock()
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
		return fmt.Errorf("lock held by other client")
	}
	if err != nil {
		return err
	}
	lock.isAcquired = true
	lock.isReleased = false
	return nil
}

// KeepAlive causes the lock to automatically extend its TTL to avoid expiration.
// This keep going until the context is cancelled, Release() is called, or the process dies.
// This calls Acquire again {beforeExpiry} before expirt.
// Don't use KeepAlive with very short TTLs, rather call Acquire yourself when you need to.
func (lock *Lock) KeepAlive(ctx context.Context, beforeExpiry time.Duration) error {
	lock.mutex.Lock()
	defer lock.mutex.Unlock()
	if !lock.isAcquired {
		return fmt.Errorf("acquire lock before keep alive")
	}
	if lock.keepAliveActive {
		return nil
	}
	if beforeExpiry >= lock.lastTTL {
		return fmt.Errorf("KeepAlive's beforeExpire (%v) should be smaller than lock's TTL (%v)", beforeExpiry, lock.lastTTL)
	}

	// Call Acquire {beforeExpiry} before lock expires
	timeLeft := lock.Expires.Add(-beforeExpiry).Sub(time.Now())
	if timeLeft <= 0 {
		timeLeft = 1 * time.Millisecond
	}
	lock.keepAliveActive = true
	time.AfterFunc(timeLeft, func() {
		lock.mutex.Lock()
		lock.keepAliveActive = false
		isReleased := lock.isReleased
		lock.mutex.Unlock()
		if !isReleased {
			lock.Acquire(ctx, lock.lastTTL)
			lock.KeepAlive(ctx, beforeExpiry)
		}
	})
	return nil
}

func (lock *Lock) release(errorIfNoop bool) error {
	lock.mutex.Lock()
	defer lock.mutex.Unlock()
	if lock.isReleased {
		if errorIfNoop {
			return fmt.Errorf("lock was already released")
		}
		return nil
	}
	ctx := context.Background()
	// Query checking that lock is still held by this client
	query := elastic.NewBoolQuery().Must(
		elastic.NewTermQuery("_id", lock.ID),
		elastic.NewTermQuery("owner.keyword", lock.Owner), // Without .keyword, this fails at matching analyzed strings (e.g. containing hyphens or spaces)
	)
	resp, err := lock.client.DeleteByQuery().Index(lock.indexName).Query(query).Refresh("true").Conflicts("proceed").Do(ctx)
	if err != nil {
		return err
	}
	lock.isReleased = true
	lock.isAcquired = false
	if errorIfNoop && resp.Deleted == 0 {
		return fmt.Errorf("release had no effect (lock: %v, client: %v)", lock.ID, lock.Owner)
	}
	return nil
}

// Release removes the lock (if it is still held).
// The only case this errors is if there's a connection error with ES.
func (lock *Lock) Release() error {
	return lock.release(false)
}

// MustRelease removes the lock (if it is still held) but returns an error if the result was a noop.
func (lock *Lock) MustRelease() error {
	return lock.release(true)
}

// IsAcquired returns if lock is acquired and not expired
func (lock *Lock) IsAcquired() bool {
	lock.mutex.Lock()
	defer lock.mutex.Unlock()
	return lock.isAcquired && lock.Expires.After(time.Now())
}

// IsReleased returns if lock was released manually or is expired
func (lock *Lock) IsReleased() bool {
	lock.mutex.Lock()
	defer lock.mutex.Unlock()
	return lock.isReleased || lock.Expires.Before(time.Now())
}
