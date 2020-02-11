package lock

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/olivere/elastic"
)

func NewElasticClient(esURL string) (*elastic.Client, error) {
	url := esURL
	if !strings.HasPrefix(url, "http") {
		url = fmt.Sprintf("http://%s", url)
	}
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	httpClient := &http.Client{Transport: tr}
	client, err := elastic.NewClient(
		elastic.SetHttpClient(httpClient),
		elastic.SetURL(url),
		elastic.SetHealthcheckTimeoutStartup(30*time.Second),
		elastic.SetSniff(false),
	)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func TestLock(t *testing.T) {
	client, err := NewElasticClient("localhost:9200")
	if err != nil {
		t.Errorf("Failed to create elastic client: %q", err)
	}
	lock := NewLock(client, "indexing-keepalive").WithOwner("client0")
	ctx := context.Background()
	if err := lock.Acquire(ctx, 1*time.Second); err != nil {
		t.Errorf("Acquire() failed: %v", err)
	}
	defer lock.Release()
}

func TestExclusiveLock(t *testing.T) {
	client, err := NewElasticClient("localhost:9200")
	if err != nil {
		t.Errorf("Failed to create elastic client: %q", err)
	}
	ctx := context.Background()
	lock := NewLock(client, "indexing-keepalive").WithOwner("client0")
	if err := lock.Acquire(ctx, 500*time.Millisecond); err != nil {
		t.Errorf("Acquire() failed: %v", err)
	}
	defer lock.Release()

	lock2 := NewLock(client, "indexing-keepalive").WithOwner("client1")
	if err := lock2.Acquire(ctx, 500*time.Millisecond); err == nil || err.Error() != "lock held by other client" {
		t.Errorf("expected error: lock should be held by other client")
	}

	// Wait for lock1 to expire and then retry lock2
	time.Sleep(500 * time.Millisecond)
	if err := lock2.Acquire(ctx, 500*time.Millisecond); err != nil {
		t.Errorf("Acquire() failed: %v", err)
	}
	defer lock2.Release()
}

func TestKeepAlive(t *testing.T) {
	client, err := NewElasticClient("localhost:9200")
	if err != nil {
		t.Errorf("Failed to create elastic client: %q", err)
	}
	lock := NewLock(client, "indexing-keepalive").WithOwner("client0")
	ctx := context.Background()
	if err := lock.Acquire(ctx, 1000*time.Millisecond); err != nil {
		t.Errorf("Acquire() failed: %v", err)
	}
	lock.KeepAlive(ctx, 250*time.Millisecond)
	time.Sleep(1100 * time.Millisecond)
	if lock.IsAcquired() == false {
		t.Errorf("IsAcquired() returned false")
	}
	if lock.Release() != nil {
		t.Errorf("Release() returned err: %v", err)
	}
	if lock.Release() != nil {
		t.Errorf("Calling Release() twice should return nil, not %v", err)
	}
	if lock.IsAcquired() != false {
		t.Errorf("IsAcquired() returned true")
	}
	if lock.IsReleased() != true {
		t.Errorf("IsReleased() returned false")
	}
}

func TestKeepAliveLater(t *testing.T) {
	client, err := NewElasticClient("localhost:9200")
	if err != nil {
		t.Errorf("Failed to create elastic client: %q", err)
	}
	lock := NewLock(client, "indexing-keepalive2").WithOwner("client0")
	ctx := context.Background()
	if err := lock.Acquire(ctx, 700*time.Millisecond); err != nil {
		t.Errorf("Acquire() failed: %v", err)
	}
	time.Sleep(800 * time.Millisecond)
	if err := lock.KeepAlive(ctx, 500*time.Millisecond); err != nil {
		t.Errorf("KeepAlive() failed: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if lock.IsAcquired() == false {
		t.Errorf("IsAcquired() returned false")
	}
	lock.Release()
}

func TestKeepAliveTooQuick(t *testing.T) {
	client, err := NewElasticClient("localhost:9200")
	if err != nil {
		t.Errorf("Failed to create elastic client: %q", err)
	}
	lock := NewLock(client, "indexing-keepalive2").WithOwner("client0")
	ctx := context.Background()
	if err := lock.Acquire(ctx, 1*time.Second); err != nil {
		t.Errorf("Acquire() failed: %v", err)
	}
	if err := lock.KeepAlive(ctx, 1*time.Second); err == nil {
		t.Errorf("KeepAlive() should return error (too short beforeExpiry)")
	}
}

func TestKeepAliveBeforeAcquire(t *testing.T) {
	client, err := NewElasticClient("localhost:9200")
	if err != nil {
		t.Errorf("Failed to create elastic client: %q", err)
	}
	lock := NewLock(client, "indexing-keepalive2").WithOwner("client0")
	ctx := context.Background()
	if err := lock.KeepAlive(ctx, 1*time.Second); err == nil {
		t.Errorf("KeepAlive() should return error (need to acquire first)")
	}
}

func TestKeepAliveMultiple(t *testing.T) {
	client, err := NewElasticClient("localhost:9200")
	if err != nil {
		t.Errorf("Failed to create elastic client: %q", err)
	}
	lock := NewLock(client, "indexing-keepalive2").WithOwner("client0")
	ctx := context.Background()
	if err := lock.Acquire(ctx, 1*time.Second); err != nil {
		t.Errorf("Acquire() failed: %v", err)
	}
	// calling KeepAlive multiple times is fine
	if err := lock.KeepAlive(ctx, 500*time.Millisecond); err != nil {
		t.Errorf("KeepAlive() returned error: %v", err)
	}
	if err := lock.KeepAlive(ctx, 500*time.Millisecond); err != nil {
		t.Errorf("KeepAlive() returned error: %v", err)
	}
}
