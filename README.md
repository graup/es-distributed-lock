Distributed Lock using Elasticsearch
====================================

[![Travis_ci](https://travis-ci.org/graup/es-distributed-lock.svg?branch=master)](https://travis-ci.org/graup/es-distributed-lock)
[![codecov](https://codecov.io/gh/graup/es-distributed-lock/branch/master/graph/badge.svg)](https://codecov.io/gh/graup/es-distributed-lock)

This go module implements a primitive distributed lock using Elasticsearch.
This is useful if you're already using ES anyway and need an easy way to add locks to improve efficiency.
For example, when you have two concurrent processes writing to a shared ES storage but want to avoid both
performing the same work. This can be used to distribute workload (processes take turns)
or for failover operation (one process does all the work, but in case it dies, another process is ready to take over).

How it works
------------

To acquire a lock, this creates a document in an ES index with an owner and expiry time.
If such a document already exists, the following rules apply:
- If the current owner is the same, set the new expiry time (this way, clients can extend their currently held locks' TTL).
- If the owner is different and the lock is expired, override owner and expiry time.
- If the owner is different and the lock is not expired, the operation is rejected.

Locks can be manually released, which deletes the document in the index if the owner still matches (this avoid errorneously deleting other client's locks).
If a lock is not released, it can be taken over by another client after it expires.

Note that this has no correctness guarantees: it is still possible (although unlikely) that more than one process acquires a lock.
However, Elasticsearch itself has consistency guarantees, so you should use
[Optimistic Concurrency Control](https://qbox.io/blog/optimistic-concurrency-control-in-elasticsearch) on the storage layer to solve data conflicts.

**Which TTL should I use?**

That depends on your availability requirements. I suggest times of 15-30 seconds.
Avoid extremely short TTLs (less than 5 seconds) as this may create timing issues. 

API
---

```go
package lock // import "github.com/graup/es-distributed-lock"

type Lock struct {
	ID       string    `json:"-"`
	Owner    string    `json:"owner"`
	Acquired time.Time `json:"acquired"`
	Expires  time.Time `json:"expires"`
}
    Lock implements a distributed lock using Elasticsearch. The use case of this
    lock is improving efficiency (not correctness)

func NewLock(client *elastic.Client, id string) *Lock
    NewLock create a new lock identified by a string

func (lock *Lock) Acquire(ctx context.Context, ttl time.Duration) error
    Acquire tries to acquire a lock with a TTL. Returns nil when succesful or
    error otherwise.

func (lock *Lock) IsAcquired() bool
    IsAcquired returns if lock is acquired and not expired

func (lock *Lock) IsReleased() bool
    IsReleased returns if lock was released manually or is expired

func (lock *Lock) KeepAlive(ctx context.Context, beforeExpiry time.Duration) error
    KeepAlive causes the lock to automatically extend its TTL to avoid
    expiration. This keep going until the context is cancelled, Release() is
    called, or the process dies. This calls Acquire again {beforeExpiry} before
    expirt. Don't use KeepAlive with very short TTLs, rather call Acquire
    yourself when you need to.

func (lock *Lock) MustRelease() error
    MustRelease removes the lock (if it is still held) but returns an error if
    the result was a noop.

func (lock *Lock) Release() error
    Release removes the lock (if it is still held). The only case this errors is
    if there's a connection error with ES.

func (lock *Lock) WithOwner(owner string) *Lock
    WithOwner is a shortcut method to set the owner manually. If you don't
    specify an owner, a random UUID is used automatically.
```

References
----------

- [Optimistic Concurrency Control in Elasticsearch. Adam Vanderbush (2017)](https://qbox.io/blog/optimistic-concurrency-control-in-elasticsearch)
- [How to do distributed locking. Martin Kleppmann (2016)](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html)
- [ElasticSearch.DistributedLock implementation in C# (2015)](https://github.com/dmombour/ElasticSearch.DistributedLock)
