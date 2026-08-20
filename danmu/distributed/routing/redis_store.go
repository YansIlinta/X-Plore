package routing

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const defaultRedisPrefix = "realtime:route:"

// RedisStore persists connection leases in Redis and indexes targets with
// sorted-set members whose scores are absolute expiry timestamps (milliseconds).
//
// Each ZSET member is one connection, encoded as gatewayID + connectionID. This
// deliberately avoids permanent distributed refcounts: if a Gateway crashes,
// its connection keys and index members age out without requiring explicit
// disconnect cleanup. Lookup lazily removes expired members before returning a
// deduplicated Gateway set.
//
// Trade-off: a very hot channel can contain many connection members. That is an
// intentional Phase 3 baseline; later experiments can justify a gateway-level
// lease optimization instead of assuming it is better in advance.
type RedisStore struct {
	client *redis.Client
	prefix string
	now    func() time.Time
}

func NewRedisStore(client *redis.Client, prefix string) (*RedisStore, error) {
	if client == nil {
		return nil, errors.New("redis client is required")
	}
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = defaultRedisPrefix
	}
	if !strings.HasSuffix(prefix, ":") {
		prefix += ":"
	}
	return &RedisStore{client: client, prefix: prefix, now: time.Now}, nil
}

func (s *RedisStore) UpsertConnection(ctx context.Context, route ConnectionRoute, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	normalized, err := validateRoute(route, ttl)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return fmt.Errorf("marshal route: %w", err)
	}

	connKey := s.connectionKey(normalized.ConnectionID)
	for attempt := 0; attempt < 3; attempt++ {
		err = s.client.Watch(ctx, func(tx *redis.Tx) error {
			var old *ConnectionRoute
			raw, getErr := tx.Get(ctx, connKey).Bytes()
			switch {
			case getErr == nil:
				var decoded ConnectionRoute
				if err := json.Unmarshal(raw, &decoded); err != nil {
					return fmt.Errorf("decode existing route %q: %w", normalized.ConnectionID, err)
				}
				old = &decoded
			case errors.Is(getErr, redis.Nil):
				// New lease.
			default:
				return getErr
			}

			expiresAt := s.now().Add(ttl)
			_, pipeErr := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				if old != nil {
					s.removeIndexMembers(ctx, pipe, *old)
				}
				s.addIndexMembers(ctx, pipe, normalized, expiresAt)
				pipe.Set(ctx, connKey, encoded, ttl)
				return nil
			})
			return pipeErr
		}, connKey)
		if errors.Is(err, redis.TxFailedErr) {
			continue
		}
		return err
	}
	return redis.TxFailedErr
}

func (s *RedisStore) RefreshConnection(ctx context.Context, connectionID string, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(connectionID) == "" {
		return errors.New("connection_id is required")
	}
	if ttl <= 0 {
		return errors.New("ttl must be positive")
	}

	connKey := s.connectionKey(connectionID)
	for attempt := 0; attempt < 3; attempt++ {
		err := s.client.Watch(ctx, func(tx *redis.Tx) error {
			raw, err := tx.Get(ctx, connKey).Bytes()
			if errors.Is(err, redis.Nil) {
				return ErrRouteNotFound
			}
			if err != nil {
				return err
			}
			var route ConnectionRoute
			if err := json.Unmarshal(raw, &route); err != nil {
				return fmt.Errorf("decode route %q: %w", connectionID, err)
			}

			expiresAt := s.now().Add(ttl)
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				s.addIndexMembers(ctx, pipe, route, expiresAt) // ZADD updates existing scores.
				pipe.Expire(ctx, connKey, ttl)
				return nil
			})
			return err
		}, connKey)
		if errors.Is(err, redis.TxFailedErr) {
			continue
		}
		return err
	}
	return redis.TxFailedErr
}

func (s *RedisStore) RemoveConnection(ctx context.Context, connectionID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(connectionID) == "" {
		return errors.New("connection_id is required")
	}

	connKey := s.connectionKey(connectionID)
	for attempt := 0; attempt < 3; attempt++ {
		err := s.client.Watch(ctx, func(tx *redis.Tx) error {
			raw, err := tx.Get(ctx, connKey).Bytes()
			if errors.Is(err, redis.Nil) {
				return nil // idempotent; expired leases are already logically absent.
			}
			if err != nil {
				return err
			}
			var route ConnectionRoute
			if err := json.Unmarshal(raw, &route); err != nil {
				return fmt.Errorf("decode route %q: %w", connectionID, err)
			}

			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				s.removeIndexMembers(ctx, pipe, route)
				pipe.Del(ctx, connKey)
				return nil
			})
			return err
		}, connKey)
		if errors.Is(err, redis.TxFailedErr) {
			continue
		}
		return err
	}
	return redis.TxFailedErr
}

func (s *RedisStore) LookupUser(ctx context.Context, userID string) ([]string, error) {
	if strings.TrimSpace(userID) == "" {
		return nil, errors.New("user_id is required")
	}
	return s.lookup(ctx, s.userKey(userID))
}

func (s *RedisStore) LookupDevice(ctx context.Context, userID, deviceID string) ([]string, error) {
	if strings.TrimSpace(userID) == "" || strings.TrimSpace(deviceID) == "" {
		return nil, errors.New("user_id and device_id are required")
	}
	return s.lookup(ctx, s.deviceKey(userID, deviceID))
}

func (s *RedisStore) LookupChannel(ctx context.Context, channelID string) ([]string, error) {
	if strings.TrimSpace(channelID) == "" {
		return nil, errors.New("channel_id is required")
	}
	return s.lookup(ctx, s.channelKey(channelID))
}

func (s *RedisStore) lookup(ctx context.Context, key string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	nowMS := s.now().UnixMilli()
	// Expiry is encoded in the score, so crash-stale members disappear without
	// requiring keyspace notifications or a global janitor.
	if err := s.client.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatInt(nowMS, 10)).Err(); err != nil {
		return nil, err
	}
	members, err := s.client.ZRangeByScore(ctx, key, &redis.ZRangeBy{
		Min: strconv.FormatInt(nowMS+1, 10),
		Max: "+inf",
	}).Result()
	if err != nil {
		return nil, err
	}

	seen := make(map[string]struct{}, len(members))
	gateways := make([]string, 0, len(members))
	for _, member := range members {
		gatewayID, _, err := decodeRouteMember(member)
		if err != nil {
			// Corrupt members are ignored rather than poisoning the entire target.
			// They will age out naturally via their score.
			continue
		}
		if _, ok := seen[gatewayID]; ok {
			continue
		}
		seen[gatewayID] = struct{}{}
		gateways = append(gateways, gatewayID)
	}
	sort.Strings(gateways)
	return gateways, nil
}

func (s *RedisStore) addIndexMembers(ctx context.Context, pipe redis.Pipeliner, route ConnectionRoute, expiresAt time.Time) {
	member := encodeRouteMember(route.GatewayID, route.ConnectionID)
	z := redis.Z{Score: float64(expiresAt.UnixMilli()), Member: member}
	pipe.ZAdd(ctx, s.userKey(route.UserID), z)
	pipe.ZAdd(ctx, s.deviceKey(route.UserID, route.DeviceID), z)
	for _, channelID := range route.ChannelIDs {
		pipe.ZAdd(ctx, s.channelKey(channelID), z)
	}
}

func (s *RedisStore) removeIndexMembers(ctx context.Context, pipe redis.Pipeliner, route ConnectionRoute) {
	member := encodeRouteMember(route.GatewayID, route.ConnectionID)
	pipe.ZRem(ctx, s.userKey(route.UserID), member)
	pipe.ZRem(ctx, s.deviceKey(route.UserID, route.DeviceID), member)
	for _, channelID := range route.ChannelIDs {
		pipe.ZRem(ctx, s.channelKey(channelID), member)
	}
}

func (s *RedisStore) connectionKey(connectionID string) string {
	return s.prefix + "conn:" + encodeKeyPart(connectionID)
}

func (s *RedisStore) userKey(userID string) string {
	return s.prefix + "user:" + encodeKeyPart(userID)
}

func (s *RedisStore) deviceKey(userID, deviceID string) string {
	return s.prefix + "device:" + encodeKeyPart(userID) + ":" + encodeKeyPart(deviceID)
}

func (s *RedisStore) channelKey(channelID string) string {
	return s.prefix + "channel:" + encodeKeyPart(channelID)
}

func encodeKeyPart(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

func encodeRouteMember(gatewayID, connectionID string) string {
	return encodeKeyPart(gatewayID) + "." + encodeKeyPart(connectionID)
}

func decodeRouteMember(member string) (gatewayID, connectionID string, err error) {
	parts := strings.SplitN(member, ".", 2)
	if len(parts) != 2 {
		return "", "", errors.New("invalid route member")
	}
	gatewayRaw, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", "", err
	}
	connectionRaw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", "", err
	}
	if len(gatewayRaw) == 0 || len(connectionRaw) == 0 {
		return "", "", errors.New("empty route member component")
	}
	return string(gatewayRaw), string(connectionRaw), nil
}
