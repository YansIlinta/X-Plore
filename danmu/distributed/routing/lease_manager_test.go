package routing

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type leaseTestStore struct {
	mu sync.Mutex

	upserts  []ConnectionRoute
	refresh  []string
	removes  []string
	lookup   map[string][]string
	upsertErr error
	refreshErr error
	removeErr error
}

func (s *leaseTestStore) UpsertConnection(_ context.Context, route ConnectionRoute, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.upserts = append(s.upserts, route)
	return s.upsertErr
}

func (s *leaseTestStore) RefreshConnection(_ context.Context, connectionID string, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refresh = append(s.refresh, connectionID)
	return s.refreshErr
}

func (s *leaseTestStore) RemoveConnection(_ context.Context, connectionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removes = append(s.removes, connectionID)
	return s.removeErr
}

func (s *leaseTestStore) LookupUser(context.Context, string) ([]string, error) { return nil, nil }
func (s *leaseTestStore) LookupDevice(context.Context, string, string) ([]string, error) { return nil, nil }
func (s *leaseTestStore) LookupChannel(context.Context, string) ([]string, error) { return nil, nil }

func testRoute(id string) ConnectionRoute {
	return ConnectionRoute{
		ConnectionID: id,
		UserID:       "u1",
		DeviceID:     "web",
		GatewayID:    "gw.example:7500",
		ChannelIDs:   []string{"danmu:room:r1"},
	}
}

func TestLeaseManagerTrackFlushAndUntrack(t *testing.T) {
	store := &leaseTestStore{}
	m, err := NewLeaseManager(store, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	route := testRoute("c1")
	m.Track(route)
	select {
	case <-m.wake:
	default:
		t.Fatal("Track should wake worker")
	}
	m.flush(context.Background())

	if len(store.upserts) != 1 || store.upserts[0].ConnectionID != "c1" {
		t.Fatalf("upserts=%+v", store.upserts)
	}

	m.Untrack("c1")
	select {
	case <-m.wake:
	default:
		t.Fatal("Untrack should wake worker")
	}
	m.flush(context.Background())
	if len(store.removes) != 1 || store.removes[0] != "c1" {
		t.Fatalf("removes=%+v", store.removes)
	}
}

func TestLeaseManagerRefreshRecreatesMissingLease(t *testing.T) {
	store := &leaseTestStore{refreshErr: ErrRouteNotFound}
	m, err := NewLeaseManager(store, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	m.Track(testRoute("c1"))
	<-m.wake
	m.flush(context.Background())
	store.upserts = nil

	m.refresh(context.Background())
	if len(store.refresh) != 1 || store.refresh[0] != "c1" {
		t.Fatalf("refresh=%+v", store.refresh)
	}
	if len(store.upserts) != 1 || store.upserts[0].ConnectionID != "c1" {
		t.Fatalf("recreate upserts=%+v", store.upserts)
	}
}

func TestLeaseManagerFailedWriteRetainsDirtyWithoutHotWake(t *testing.T) {
	store := &leaseTestStore{upsertErr: errors.New("redis down")}
	m, err := NewLeaseManager(store, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	m.Track(testRoute("c1"))
	<-m.wake // consume the lifecycle wake; flush must not enqueue another one on failure.
	m.flush(context.Background())

	m.mu.Lock()
	_, dirty := m.dirty["c1"]
	m.mu.Unlock()
	if !dirty {
		t.Fatal("failed upsert must remain dirty for periodic retry")
	}
	select {
	case <-m.wake:
		t.Fatal("failed upsert must not immediately re-wake worker and hot-loop")
	default:
	}
}

func TestLeaseManagerValidation(t *testing.T) {
	if _, err := NewLeaseManager(nil, time.Second); err == nil {
		t.Fatal("nil store must fail")
	}
	if _, err := NewLeaseManager(&leaseTestStore{}, 0); err == nil {
		t.Fatal("non-positive ttl must fail")
	}
}
