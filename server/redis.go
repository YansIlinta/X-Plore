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

// Publish 将消息发布到 Redis Pub/Sub
func (rh *RedisHub) Publish(msg *Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	channel := fmt.Sprintf("room:%s", msg.RoomID)
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
			var msg Message
			if err := json.Unmarshal([]byte(redisMsg.Payload), &msg); err != nil {
				log.Printf("[RedisHub] unmarshal error: %v", err)
				continue
			}

			// 跳过本机发出的消息，避免重复广播
			if msg.SourceServer == rh.serverID {
				continue
			}

			// 广播到本机该房间的连接
			data, err := json.Marshal([]*Message{&msg})
			if err != nil {
				continue
			}
			rh.hub.BroadcastToRoom(msg.RoomID, data)
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
			var msg Message
			if err := json.Unmarshal([]byte(redisMsg.Payload), &msg); err != nil {
				log.Printf("[RedisHub] unmarshal error: %v", err)
				continue
			}

			// 跳过本机发出的消息
			if msg.SourceServer == rh.serverID {
				continue
			}

			data, err := json.Marshal([]*Message{&msg})
			if err != nil {
				continue
			}
			rh.hub.BroadcastToRoom(msg.RoomID, data)
		}
	}
}

// Close 关闭 Redis 连接
func (rh *RedisHub) Close() error {
	return rh.client.Close()
}
