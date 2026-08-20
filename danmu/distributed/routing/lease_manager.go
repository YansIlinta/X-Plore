package routing

import (
	"context"
	"errors"
	"sync"
	"time"
)

// LeaseManager keeps the RouteStore synchronized with a Gateway's local
// connection lifecycle without performing external I/O on the WebSocket
// AddClient/RemoveClient path.
//
// Track/Untrack only mutate bounded in-process state and signal one worker.
// The worker flushes dirty changes and periodically refreshes active leases.
// Failed writes remain dirty and are retried on the next periodic tick; they do
// not immediately re-signal the worker, which avoids a hot retry loop while
// Redis is unavailable. A missing lease during refresh is recreated with
// UpsertConnection. Clean disconnects therefore remove routes quickly while TTL
// remains the crash-recovery mechanism.
type LeaseManager struct {
	store Store
	ttl   time.Duration

	mu      sync.Mutex
	active  map[string]ConnectionRoute
	dirty   map[string]struct{}
	removed map[string]struct{}
	wake    chan struct{}
}

func NewLeaseManager(store Store, ttl time.Duration) (*LeaseManager, error) {
	if store == nil {
		return nil, errors.New("route store is required")
	}
	if ttl <= 0 {
		return nil, errors.New("route ttl must be positive")
	}
	return &LeaseManager{
		store:   store,
		ttl:     ttl,
		active:  make(map[string]ConnectionRoute),
		dirty:   make(map[string]struct{}),
		removed: make(map[string]struct{}),
		wake:    make(chan struct{}, 1),
	}, nil
}

// Track records the latest route for a local connection. It does no Store I/O
// and is safe to call from a Hub lifecycle hook.
func (m *LeaseManager) Track(route ConnectionRoute) {
	if route.ConnectionID == "" {
		return
	}
	m.mu.Lock()
	m.active[route.ConnectionID] = route
	m.dirty[route.ConnectionID] = struct{}{}
	delete(m.removed, route.ConnectionID)
	m.mu.Unlock()
	m.signal()
}

// Untrack marks a connection for removal. Repeated calls are idempotent.
func (m *LeaseManager) Untrack(connectionID string) {
	if connectionID == "" {
		return
	}
	m.mu.Lock()
	delete(m.active, connectionID)
	delete(m.dirty, connectionID)
	m.removed[connectionID] = struct{}{}
	m.mu.Unlock()
	m.signal()
}

func (m *LeaseManager) signal() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

// Run blocks until ctx is canceled. Refresh runs at ttl/3, clamped to at least
// one second so very small test TTLs do not spin. Dirty events are flushed as
// soon as the worker is signaled.
func (m *LeaseManager) Run(ctx context.Context) {
	interval := m.ttl / 3
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.wake:
			m.flush(ctx)
		case <-ticker.C:
			m.flush(ctx)
			m.refresh(ctx)
		}
	}
}

// flush is intentionally unexported but deterministic for package tests.
func (m *LeaseManager) flush(ctx context.Context) {
	if err := ctx.Err(); err != nil {
		return
	}

	m.mu.Lock()
	removed := make([]string, 0, len(m.removed))
	for connectionID := range m.removed {
		removed = append(removed, connectionID)
	}
	dirty := make([]ConnectionRoute, 0, len(m.dirty))
	for connectionID := range m.dirty {
		if route, ok := m.active[connectionID]; ok {
			dirty = append(dirty, route)
		}
	}
	m.removed = make(map[string]struct{})
	m.dirty = make(map[string]struct{})
	m.mu.Unlock()

	for _, connectionID := range removed {
		if err := m.store.RemoveConnection(ctx, connectionID); err != nil {
			m.requeueRemove(connectionID)
		}
	}
	for _, route := range dirty {
		if err := m.store.UpsertConnection(ctx, route, m.ttl); err != nil {
			m.requeueDirty(route.ConnectionID)
		}
	}
}

func (m *LeaseManager) refresh(ctx context.Context) {
	if err := ctx.Err(); err != nil {
		return
	}
	m.mu.Lock()
	routes := make([]ConnectionRoute, 0, len(m.active))
	for _, route := range m.active {
		routes = append(routes, route)
	}
	m.mu.Unlock()

	for _, route := range routes {
		err := m.store.RefreshConnection(ctx, route.ConnectionID, m.ttl)
		if errors.Is(err, ErrRouteNotFound) {
			if upsertErr := m.store.UpsertConnection(ctx, route, m.ttl); upsertErr != nil {
				m.requeueDirty(route.ConnectionID)
			}
		}
		// Other transient errors are retried by the next refresh tick; the Store
		// TTL remains the authoritative liveness bound.
	}
}

func (m *LeaseManager) requeueDirty(connectionID string) {
	m.mu.Lock()
	if _, active := m.active[connectionID]; active {
		m.dirty[connectionID] = struct{}{}
	}
	m.mu.Unlock()
	// Deliberately do not signal here. A Store outage must not turn into a hot
	// retry loop; the next periodic tick retries the retained dirty entry.
}

func (m *LeaseManager) requeueRemove(connectionID string) {
	m.mu.Lock()
	if _, active := m.active[connectionID]; !active {
		m.removed[connectionID] = struct{}{}
	}
	m.mu.Unlock()
	// See requeueDirty: retry on the next periodic tick, not immediately.
}
