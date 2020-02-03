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
	if err := lock.Acquire(ctx, 1); err != nil {
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
	if err := lock.Acquire(ctx, 1); err != nil {
		t.Errorf("Acquire() failed: %v", err)
	}
	defer lock.Release()

	lock2 := NewLock(client, "indexing-keepalive").WithOwner("client1")
	if err := lock2.Acquire(ctx, 1); err == nil || err.Error() != "lock held by other client" {
		t.Errorf("expected error: lock should be held by other client")
	}

	// Wait for lock1 to expire
	time.Sleep(1 * time.Second)
	if err := lock2.Acquire(ctx, 1); err != nil {
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
	if err := lock.Acquire(ctx, 5); err != nil {
		t.Errorf("Acquire() failed: %v", err)
	}
	lock.KeepAlive(ctx)
	time.Sleep(10 * time.Second)
	lock.Release()
	time.Sleep(5 * time.Second)
}
