package lock

import (
	"context"
	"errors"
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
	lastTTL         int32
	ID              string    `json:"-"`
	Owner           string    `json:"owner"`
	Acquired        time.Time `json:"acquired"`
	Expires         time.Time `json:"expires"`
	IsAcquired      bool      `json:"-"`
	IsReleased      bool      `json:"-"`
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
		IsAcquired:      false,
		IsReleased:      false,
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
func (lock *Lock) Acquire(ctx context.Context, ttl int32) error {
	lock.lastTTL = ttl
	lock.Acquired = time.Now()
	lock.Expires = lock.Acquired.Add(time.Duration(ttl) * time.Second)
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
	lock.IsAcquired = true
	return nil
}

// KeepAlive causes the lock to automatically extend its TTL to avoid expiration.
// This keep going until the context is cancelled, Release() is called, or the process dies.
// Don't use KeepAlive with very short TTLs.
func (lock *Lock) KeepAlive(ctx context.Context) error {
	if !lock.IsAcquired {
		return errors.New("acquire lock before keep alive")
	}
	if lock.keepAliveActive {
		return nil
	}

	// Call Acquire when lock expired 90%
	seconds := 0.1 * float32(lock.lastTTL)
	timeLeft := lock.Expires.Add(time.Duration(-seconds) * time.Second).Sub(time.Now())
	if timeLeft < 0 {
		timeLeft = 0
	}
	lock.keepAliveActive = true
	time.AfterFunc(timeLeft, func() {
		lock.keepAliveActive = false
		if !lock.IsReleased {
			lock.Acquire(ctx, lock.lastTTL)
			lock.KeepAlive(ctx)
		}
	})
	return nil
}

// Release removes the lock (if it is still held)
func (lock *Lock) Release() error {
	if lock.IsReleased {
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
	lock.IsReleased = true
	lock.IsAcquired = false
	return nil
}
