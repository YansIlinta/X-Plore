package routing

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
)

var ErrRouteNotFound = errors.New("route not found")

// ConnectionRoute is the lease unit for the routing plane.
//
// The store registers connections, not coarse user/gateway pairs. User/device/
// channel routes are derived indexes with reference counts. This matters for
// multi-device/multi-tab semantics: if two connections for the same user live on
// one Gateway, removing one connection must not remove the user's Gateway route
// while the other connection is still alive.
type ConnectionRoute struct {
	ConnectionID string
	UserID       string
	DeviceID     string
	GatewayID    string
	ChannelIDs   []string
}

// Store is the routing-plane contract used by Phase 3. Redis will implement the
// same semantics later; MemoryStore is a deterministic baseline for tests and
// local experiments.
type Store interface {
	UpsertConnection(ctx context.Context, route ConnectionRoute, ttl time.Duration) error
	RefreshConnection(ctx context.Context, connectionID string, ttl time.Duration) error
	RemoveConnection(ctx context.Context, connectionID string) error

	LookupUser(ctx context.Context, userID string) ([]string, error)
	LookupDevice(ctx context.Context, userID, deviceID string) ([]string, error)
	LookupChannel(ctx context.Context, channelID string) ([]string, error)
}

type memoryEntry struct {
	route     ConnectionRoute
	expiresAt time.Time
}

type gatewayRefs map[string]int

// MemoryStore keeps connection leases plus refcounted derived indexes. Expired
// entries are removed lazily on mutations/lookups; the production Redis store
// will use Redis TTL for crash recovery.
type MemoryStore struct {
	mu sync.Mutex

	now func() time.Time

	connections map[string]memoryEntry
	users       map[string]gatewayRefs
	devices     map[string]gatewayRefs
	channels    map[string]gatewayRefs
}

func NewMemoryStore() *MemoryStore {
	return newMemoryStore(time.Now)
}

func newMemoryStore(now func() time.Time) *MemoryStore {
	if now == nil {
		now = time.Now
	}
	return &MemoryStore{
		now:         now,
		connections: make(map[string]memoryEntry),
		users:       make(map[string]gatewayRefs),
		devices:     make(map[string]gatewayRefs),
		channels:    make(map[string]gatewayRefs),
	}
}

func validateRoute(route ConnectionRoute, ttl time.Duration) (ConnectionRoute, error) {
	if strings.TrimSpace(route.ConnectionID) == "" {
		return ConnectionRoute{}, errors.New("connection_id is required")
	}
	if strings.TrimSpace(route.UserID) == "" {
		return ConnectionRoute{}, errors.New("user_id is required")
	}
	if strings.TrimSpace(route.DeviceID) == "" {
		return ConnectionRoute{}, errors.New("device_id is required")
	}
	if strings.TrimSpace(route.GatewayID) == "" {
		return ConnectionRoute{}, errors.New("gateway_id is required")
	}
	if ttl <= 0 {
		return ConnectionRoute{}, errors.New("ttl must be positive")
	}

	// Normalize channels so a repeated channel in one binding cannot inflate
	// refcounts. Empty channel IDs are rejected because they create an
	// unqueryable route key.
	seen := make(map[string]struct{}, len(route.ChannelIDs))
	channels := make([]string, 0, len(route.ChannelIDs))
	for _, raw := range route.ChannelIDs {
		ch := strings.TrimSpace(raw)
		if ch == "" {
			return ConnectionRoute{}, errors.New("channel_id must not be empty")
		}
		if _, ok := seen[ch]; ok {
			continue
		}
		seen[ch] = struct{}{}
		channels = append(channels, ch)
	}
	sort.Strings(channels)
	route.ChannelIDs = channels
	return route, nil
}

func (s *MemoryStore) UpsertConnection(ctx context.Context, route ConnectionRoute, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	normalized, err := validateRoute(route, ttl)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(s.now())

	if old, ok := s.connections[normalized.ConnectionID]; ok {
		s.removeEntryLocked(old)
	}
	entry := memoryEntry{route: normalized, expiresAt: s.now().Add(ttl)}
	s.connections[normalized.ConnectionID] = entry
	s.addEntryLocked(entry)
	return nil
}

func (s *MemoryStore) RefreshConnection(ctx context.Context, connectionID string, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(connectionID) == "" {
		return errors.New("connection_id is required")
	}
	if ttl <= 0 {
		return errors.New("ttl must be positive")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.purgeExpiredLocked(now)
	entry, ok := s.connections[connectionID]
	if !ok {
		return ErrRouteNotFound
	}
	entry.expiresAt = now.Add(ttl)
	s.connections[connectionID] = entry
	return nil
}

// RemoveConnection is idempotent. Disconnect cleanup can therefore race with
// lease expiry without creating an error path.
func (s *MemoryStore) RemoveConnection(ctx context.Context, connectionID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(connectionID) == "" {
		return errors.New("connection_id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(s.now())
	entry, ok := s.connections[connectionID]
	if !ok {
		return nil
	}
	s.removeEntryLocked(entry)
	delete(s.connections, connectionID)
	return nil
}

func (s *MemoryStore) LookupUser(ctx context.Context, userID string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(userID) == "" {
		return nil, errors.New("user_id is required")
	}
	return s.lookup(ctx, s.users, userID)
}

func (s *MemoryStore) LookupDevice(ctx context.Context, userID, deviceID string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(userID) == "" || strings.TrimSpace(deviceID) == "" {
		return nil, errors.New("user_id and device_id are required")
	}
	return s.lookup(ctx, s.devices, deviceKey(userID, deviceID))
}

func (s *MemoryStore) LookupChannel(ctx context.Context, channelID string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(channelID) == "" {
		return nil, errors.New("channel_id is required")
	}
	return s.lookup(ctx, s.channels, channelID)
}

func (s *MemoryStore) lookup(ctx context.Context, index map[string]gatewayRefs, key string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(s.now())
	refs := index[key]
	out := make([]string, 0, len(refs))
	for gatewayID, count := range refs {
		if count > 0 {
			out = append(out, gatewayID)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (s *MemoryStore) addEntryLocked(entry memoryEntry) {
	r := entry.route
	incRef(s.users, r.UserID, r.GatewayID)
	incRef(s.devices, deviceKey(r.UserID, r.DeviceID), r.GatewayID)
	for _, ch := range r.ChannelIDs {
		incRef(s.channels, ch, r.GatewayID)
	}
}

func (s *MemoryStore) removeEntryLocked(entry memoryEntry) {
	r := entry.route
	decRef(s.users, r.UserID, r.GatewayID)
	decRef(s.devices, deviceKey(r.UserID, r.DeviceID), r.GatewayID)
	for _, ch := range r.ChannelIDs {
		decRef(s.channels, ch, r.GatewayID)
	}
}

func (s *MemoryStore) purgeExpiredLocked(now time.Time) {
	for connectionID, entry := range s.connections {
		if now.Before(entry.expiresAt) {
			continue
		}
		s.removeEntryLocked(entry)
		delete(s.connections, connectionID)
	}
}

func incRef(index map[string]gatewayRefs, key, gatewayID string) {
	refs := index[key]
	if refs == nil {
		refs = make(gatewayRefs)
		index[key] = refs
	}
	refs[gatewayID]++
}

func decRef(index map[string]gatewayRefs, key, gatewayID string) {
	refs := index[key]
	if refs == nil {
		return
	}
	if refs[gatewayID] <= 1 {
		delete(refs, gatewayID)
	} else {
		refs[gatewayID]--
	}
	if len(refs) == 0 {
		delete(index, key)
	}
}

func deviceKey(userID, deviceID string) string {
	return userID + "\x00" + deviceID
}
