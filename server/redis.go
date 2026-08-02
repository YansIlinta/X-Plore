package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/redis/go-redis/v9"
)

// RedisHub 管理 Redis Pub/Sub 跨机实时广播
// 频道名格式：room:{roomId}
// 每条消息带 SourceServer 字段，订阅回来时若是本机发的则跳过，避免重复广播
type RedisHub struct {
	client   *redis.Client
	hub      *Hub
	serverID string
	ctx      context.Context
}

func NewRedisHub(addr, password string, db int, hub *Hub, ctx context.Context) (*RedisHub, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
		PoolSize: 100,
	})

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	rh := &RedisHub{
		client:   client,
		hub:      hub,
		serverID: hub.serverID,
		ctx:      ctx,
	}

	return rh, nil
}

// PublishBatch 将同一房间的一批消息序列化后单次发布到 Redis Pub/Sub
// data 与广播给客户端的 payload 复用同一份（[]byte，[]Message 的 JSON 数组），
// 避免逐条 Marshal + PUBLISH 造成的序列化开销和 Redis RTT 放大
func (rh *RedisHub) PublishBatch(roomID string, data []byte) error {
	channel := fmt.Sprintf("room:%s", roomID)
	return rh.client.Publish(rh.ctx, channel, data).Err()
}

// SubscribePattern 用模式订阅所有房间频道
func (rh *RedisHub) SubscribePattern() {
	sub := rh.client.PSubscribe(rh.ctx, "room:*")
	defer sub.Close()

	ch := sub.Channel()
	for {
		select {
		case <-rh.ctx.Done():
			return
		case redisMsg, ok := <-ch:
			if !ok {
				return
			}
			rh.handleIncoming(redisMsg.Channel, redisMsg.Payload)
		}
	}
}

// handleIncoming 处理一条 Redis Pub/Sub 载荷（一个房间一批消息的 JSON 数组）
// 优化：channel 名形如 room:{roomId}，先据此判断本机是否持有该房间——
// 若不持有则直接丢弃，避免对"注定要广播给别人房间"的 payload 做无谓的 JSON 反序列化
// （模式订阅会收到全网所有房间的消息，N 台机 × 全量消息的解析开销是横向扩展的瓶颈）。
// 只有本机持有该房间时才反序列化，并看首条的 SourceServer 判断是否为本机发出（避免回环）。
func (rh *RedisHub) handleIncoming(channel, payload string) {
	roomID := strings.TrimPrefix(channel, "room:")
	if roomID == "" || !rh.hub.HasRoom(roomID) {
		return
	}

	var msgs []*Message
	if err := json.Unmarshal([]byte(payload), &msgs); err != nil {
		log.Printf("[RedisHub] unmarshal error: %v", err)
		return
	}
	if len(msgs) == 0 || msgs[0].SourceServer == rh.serverID {
		return
	}
	// payload 已经是目标广播格式（[]Message 的 JSON 数组），直接转发给客户端，无需重新 Marshal
	rh.hub.BroadcastToRoom(roomID, []byte(payload))
}

// Close 关闭 Redis 连接
func (rh *RedisHub) Close() error {
	return rh.client.Close()
}
