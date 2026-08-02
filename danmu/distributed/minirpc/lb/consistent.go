// Package lb 实现一致性哈希环（带虚拟节点）。
//
// 解决的问题：hash(key) % N 在 N 变化时几乎全部 key 重映射（缓存全灭）；
// 一致性哈希把 key 和节点映射到同一个 2^32 环上，key 顺时针归属最近节点，
// 增删节点只影响环上相邻的一段（约 1/N 的 key）。
//
// 虚拟节点：每个真实节点以 "addr#0"、"addr#1"… 的形式上环 replicas 次，
// 让分布更均匀，且节点下线时其负载摊给所有幸存者而非单个邻居。
package lb

import (
	"hash/crc32"
	"sort"
	"strconv"
)

type Ring struct {
	replicas int               // 每个真实节点的虚拟节点数
	keys     []uint32          // 环上所有虚拟节点的哈希值，升序
	owner    map[uint32]string // 虚拟节点哈希 → 真实节点地址
}

func NewRing(replicas int) *Ring {
	return &Ring{replicas: replicas, owner: make(map[uint32]string)}
}

func hash(s string) uint32 {
	return crc32.ChecksumIEEE([]byte(s))
}

// Reset 用一组节点重建整个环。节点列表来自服务发现，变了就整体重建——
// 简单可靠；工业实现会做增量 Add/Remove，思想一样。
func (r *Ring) Reset(addrs []string) {
	r.keys = r.keys[:0]
	clear(r.owner)
	for _, addr := range addrs {
		for i := 0; i < r.replicas; i++ {
			h := hash(addr + "#" + strconv.Itoa(i))
			r.keys = append(r.keys, h)
			r.owner[h] = addr
		}
	}
	sort.Slice(r.keys, func(i, j int) bool { return r.keys[i] < r.keys[j] })
}

// Get 返回 key 顺时针方向遇到的第一个节点。
func (r *Ring) Get(key string) string {
	if len(r.keys) == 0 {
		return ""
	}
	h := hash(key)
	// 二分找第一个 >= h 的虚拟节点；越过末尾则绕回环首（这就是"环"）
	i := sort.Search(len(r.keys), func(i int) bool { return r.keys[i] >= h })
	if i == len(r.keys) {
		i = 0
	}
	return r.owner[r.keys[i]]
}
