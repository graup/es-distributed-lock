package lock

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/olivere/elastic"
)

type Lock struct {
	client     *elastic.Client
	indexName  string
	typeName   string
	lastTtl    int32
	ID         string    `json:"-"`
	Owner      string    `json:"owner"`
	Acquired   time.Time `json:"acquired"`
	Expires    time.Time `json:"expires"`
	IsAcquired bool      `json:"-"`
	IsReleased bool      `json:"-"`
}

var (
	clientID = uuid.New().String()
)

func NewLock(client *elastic.Client, id string) *Lock {
	return &Lock{
		client:     client,
		ID:         id,
		Owner:      clientID,
		indexName:  "distributed-locks",
		typeName:   "lock",
		IsAcquired: false,
		IsReleased: false,
	}
}

func (lock *Lock) WithOwner(owner string) *Lock {
	lock.Owner = owner
	return lock
}

func (lock *Lock) Acquire(ctx context.Context, ttl int32) error {
	lock.lastTtl = ttl
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

// KeepAlive causes the lock to automatically extend its TTL to avoid expiration
func (lock *Lock) KeepAlive(ctx context.Context) error {
	if !lock.IsAcquired {
		return errors.New("acquire lock before keep alive")
	}

	// Kall Acquire again a few seconds before lock expires
	seconds := 0.1 * float32(lock.lastTtl)
	timeLeft := lock.Expires.Add(time.Duration(-seconds) * time.Second).Sub(time.Now())
	fmt.Printf("keep alive with %.0f seconds left", timeLeft.Seconds())
	if timeLeft < 0 {
		timeLeft = 0
	}
	time.AfterFunc(timeLeft, func() {
		fmt.Printf("keep alive fired. %v", lock.IsReleased)
		if !lock.IsReleased {
			lock.Acquire(ctx, lock.lastTtl)
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
