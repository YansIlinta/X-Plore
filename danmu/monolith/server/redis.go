package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/redis/go-redis/v9"
)

// RedisHub 管理 Redis 跨机实时广播。
//
// 默认使用 Redis 7 sharded Pub/Sub（SPUBLISH/SSUBSCRIBE）：
//   - 频道名：danmu{s}:mux，s = fnv32(roomID) % shardCount（默认 8）。分片键是
//     频道名第一个冒号前的部分（Redis sharded Pub/Sub 规则），因此 8 个频道是
//     固定复用集合，房间按哈希落到其中一个，房间维度信息在消息 payload 里。
//   - 订阅：SSUBSCRIBE 全部 shardCount 个精确频道（sharded Pub/Sub 不支持
//     pattern），Redis 侧从「每条消息 × 全量 pattern 匹配」降为精确频道直查；
//     集群模式下消息只留在分片所在节点、只投递给该分片订阅者，吞吐随分片数
//     线性扩展（Centrifugo 2026-06 实测：单机 ~650k msg/s 上限、经典 cluster
//     Pub/Sub 随节点数退化、8 shard ~5.2M msg/s，见 centrifugal.dev 博客
//     "Scaling Redis Pub/Sub to Millions of Channels"，2026-06-29）。
//   - -redis-sharded=false 时回退经典 room:{id} + PSUBSCRIBE room:* 路径。
//
// 每条消息带 SourceServer 字段，订阅回来时若是本机发的则跳过，避免重复广播。
// 兼容性说明：Dragonfly 实现了 SPUBLISH/SSUBSCRIBE 命令族，可作 Redis 替代。
type RedisHub struct {
	client     *redis.Client
	hub        *Hub
	serverID   string
	ctx        context.Context
	shardCount int
	useSharded bool
}

const (
	defaultShardCount = 8
	shardedChanKey    = "danmu"      // 分片频道第一段（= 分片键）
	shardedChanSuffix = ":mux"       // 固定复用频道名后缀
	ctrlChannel       = "danmu-ctrl" // 控制面跨机频道（踢人/关房/慢速模式）
)

func NewRedisHub(addr, password string, db int, hub *Hub, ctx context.Context, shardCount int, useSharded bool) (*RedisHub, error) {
	if shardCount <= 0 {
		shardCount = defaultShardCount
	}
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
		client:     client,
		hub:        hub,
		serverID:   hub.serverID,
		ctx:        ctx,
		shardCount: shardCount,
		useSharded: useSharded,
	}

	return rh, nil
}

// roomChannel 返回房间消息应发布到的频道名。
func (rh *RedisHub) roomChannel(roomID string) string {
	if rh.useSharded {
		shard := fnv32(roomID) % uint32(rh.shardCount)
		return fmt.Sprintf("%s%d%s", shardedChanKey, shard, shardedChanSuffix)
	}
	return fmt.Sprintf("room:%s", roomID)
}

// subscribeChannels 返回订阅侧需要订阅的完整频道列表。
func (rh *RedisHub) subscribeChannels() []string {
	if rh.useSharded {
		channels := make([]string, rh.shardCount+1)
		for i := 0; i < rh.shardCount; i++ {
			channels[i] = fmt.Sprintf("%s%d%s", shardedChanKey, i, shardedChanSuffix)
		}
		channels[rh.shardCount] = ctrlChannel
		return channels
	}
	return []string{"room:*", ctrlChannel}
}

// PublishBatch 将同一房间的一批消息序列化后单次发布到 Redis（sharded）Pub/Sub。
// data 与广播给客户端的 payload 复用同一份（[]byte，[]Message 的 JSON 数组），
// 避免逐条 Marshal + PUBLISH 造成的序列化开销和 Redis RTT 放大。
func (rh *RedisHub) PublishBatch(roomID string, data []byte) error {
	channel := rh.roomChannel(roomID)
	if rh.useSharded {
		return rh.client.SPublish(rh.ctx, channel, data).Err()
	}
	return rh.client.Publish(rh.ctx, channel, data).Err()
}

// SubscribeLoop 订阅全部频道并处理入站消息。
// sharded 模式订阅全部固定复用频道 + 控制频道；经典模式用 pattern 订阅所有房间频道 + 控制频道。
func (rh *RedisHub) SubscribeLoop() {
	var sub *redis.PubSub
	if rh.useSharded {
		sub = rh.client.SSubscribe(rh.ctx, rh.subscribeChannels()...)
	} else {
		// 经典模式：PSUBSCRIBE 支持多个 pattern；ctrl 频道是精确名，pattern 也能匹配
		sub = rh.client.PSubscribe(rh.ctx, rh.subscribeChannels()...)
	}
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

// PublishCtrl 经控制频道广播跨机控制面动作（踢人/关房/慢速模式）。
// sharded 模式用 SPublish（与 SSubscribe 配对），经典模式用 Publish。
func (rh *RedisHub) PublishCtrl(msg ctrlMsg) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if rh.useSharded {
		return rh.client.SPublish(rh.ctx, ctrlChannel, data).Err()
	}
	return rh.client.Publish(rh.ctx, ctrlChannel, data).Err()
}

// handleIncoming 处理一条 Redis Pub/Sub 载荷。
// 控制频道 → 跨机控制面；否则按房间广播处理。
func (rh *RedisHub) handleIncoming(channel, payload string) {
	if channel == ctrlChannel {
		var msg ctrlMsg
		if err := json.Unmarshal([]byte(payload), &msg); err != nil {
			log.Printf("[RedisHub] ctrl unmarshal error: %v", err)
			return
		}
		rh.hub.handleCtrl(msg)
		return
	}

	// 优化：先廉价地定位房间（经典模式从频道名；sharded 复用频道从 payload 里扫
	// 第一个 room_id，不做完整反序列化），本机不持有该房间则直接丢弃——
	// N 台机 × 全量消息里，绝大多数 payload 注定要丢弃，完整 JSON 反序列化的
	// 分配开销是横向扩展的瓶颈。
	roomID := ""
	if strings.HasPrefix(channel, "room:") {
		roomID = strings.TrimPrefix(channel, "room:")
	} else {
		roomID = extractFirstRoomID([]byte(payload))
	}
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
	// 采纳跨机消息携带的序号并写入本地热历史（序号由源机打号，本机只取最大值），
	// 使本机客户端的重连补发同样覆盖经 Redis 到达的消息。
	var maxSeq uint64
	for _, m := range msgs {
		if m.Seq > maxSeq {
			maxSeq = m.Seq
		}
	}
	rh.hub.adoptRoomSeq(roomID, maxSeq)
	rh.hub.hist.AppendBatch(roomID, msgs)
	// payload 已经是目标广播格式（[]Message 的 JSON 数组），直接转发给客户端，
	// 无需重新 Marshal；按批次首条 priority 路由到普通/高优通道
	if msgs[0].Priority > 0 {
		rh.hub.BroadcastToRoomHigh(roomID, []byte(payload))
	} else {
		rh.hub.BroadcastToRoom(roomID, []byte(payload))
	}
}

// extractFirstRoomID 从 payload 中扫出第一条消息的 room_id，避免完整反序列化。
// payload 形如 [{"type":"danmu","msg_id":...,"room_id":"room-1",...},...]。
func extractFirstRoomID(payload []byte) string {
	const key = `"room_id"`
	idx := bytes.Index(payload, []byte(key))
	if idx < 0 {
		return ""
	}
	rest := payload[idx+len(key):]
	colon := bytes.IndexByte(rest, ':')
	if colon < 0 {
		return ""
	}
	val := rest[colon+1:]
	val = bytes.TrimLeft(val, ` "`)
	end := bytes.IndexAny(val, `",}`)
	if end < 0 {
		return ""
	}
	return string(val[:end])
}

// Close 关闭 Redis 连接
func (rh *RedisHub) Close() error {
	return rh.client.Close()
}
