package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

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

// SubscribeRoom 订阅指定房间的频道
func (rh *RedisHub) SubscribeRoom(roomID string) {
	channel := fmt.Sprintf("room:%s", roomID)
	sub := rh.client.Subscribe(rh.ctx, channel)
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
			rh.handleIncoming(redisMsg.Payload)
		}
	}
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
			rh.handleIncoming(redisMsg.Payload)
		}
	}
}

// handleIncoming 处理一条 Redis Pub/Sub 载荷（一个房间一批消息的 JSON 数组）
// 一批消息都由同一个 worker.flush() 在本机生成，SourceServer 恒相同，
// 只需看首条即可判断是否为本机发出（避免回环重复广播）
func (rh *RedisHub) handleIncoming(payload string) {
	var msgs []*Message
	if err := json.Unmarshal([]byte(payload), &msgs); err != nil {
		log.Printf("[RedisHub] unmarshal error: %v", err)
		return
	}
	if len(msgs) == 0 || msgs[0].SourceServer == rh.serverID {
		return
	}
	// payload 已经是目标广播格式（[]Message 的 JSON 数组），直接转发给客户端，无需重新 Marshal
	rh.hub.BroadcastToRoom(msgs[0].RoomID, []byte(payload))
}

// Close 关闭 Redis 连接
func (rh *RedisHub) Close() error {
	return rh.client.Close()
}
