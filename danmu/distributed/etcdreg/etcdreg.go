// Package etcdreg 封装本项目在 etcd 上的服务注册/发现约定，替代原自研 registry/。
//
// 数据模型（机制与原 registry 同源：注册=续租心跳，没有注销接口）：
//
//	key:    danmu/services/<service>/<addr>
//	value:  {"Op":0,"Addr":"<addr>","Metadata":null}
//	租约:   TTL 10s，客户端持续 KeepAlive；进程崩溃/退出后 lease 到期 key 自动消失，
//	        所以不提供注销接口——崩溃时根本来不及注销，活性判断只该靠租约。
//
// key 不带前导斜杠、value 用 grpc naming.Update 兼容 JSON，是刻意为之：
// comet→logic 直接消费 etcd 官方 naming/resolver（它对 "<prefix>/" 做 Get+Watch，
// 地址从 value 的 Addr 字段解析），本项目自有的 List/ListAll/Watch 则从
// key 的最后一段取地址，两套读取方式在同一份数据上并存。
package etcdreg

import (
	"context"
	"encoding/json"
	"log"
	"sort"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// ServicePrefix 是所有注册 key 的公共前缀（无前导斜杠，见包注释）。
const ServicePrefix = "danmu/services/"

// endpointValue 与 etcd naming/endpoints 的 internal.Update JSON 格式一致，
// 使 key 能被官方 naming/resolver 直接消费。
type endpointValue struct {
	Op       uint8  `json:"Op"` // 0=Add，见 naming/endpoints/internal.Operation
	Addr     string `json:"Addr"`
	Metadata any    `json:"Metadata"`
}

// ServiceKey 返回 service 下 addr 的注册 key。
func ServiceKey(service, addr string) string {
	return ServicePrefix + service + "/" + addr
}

// Register 把 addr 注册进 service 并持续续租，直到 ctx 取消（取消时主动 Revoke，
// 立即从发现面消失，不必等租约过期）。返回的 done 通道在续租结束
// （ctx 取消或租约失效）时关闭，供 KeepAlive 判断是否需要重新注册。
func Register(ctx context.Context, cli *clientv3.Client, service, addr string, ttl time.Duration) (<-chan struct{}, error) {
	if ttl <= 0 {
		ttl = 10 * time.Second // 防 Grant(<=0) 报错
	}
	lease, err := cli.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return nil, err
	}
	value, err := json.Marshal(endpointValue{Addr: addr})
	if err != nil {
		return nil, err
	}
	if _, err := cli.Put(ctx, ServiceKey(service, addr), string(value), clientv3.WithLease(lease.ID)); err != nil {
		return nil, err
	}
	ka, err := cli.KeepAlive(ctx, lease.ID)
	if err != nil {
		return nil, err
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			revCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if _, err := cli.Revoke(revCtx, lease.ID); err != nil {
				log.Printf("[etcdreg] revoke %s/%s: %v", service, addr, err)
			}
		}()
		for range ka { // 必须消费 keepalive 响应，否则续租会停
		}
	}()
	return done, nil
}

// KeepAlive 语义与原自研 registry.KeepAlive 一致：立即注册并持续续租。
// 注册失败每 2s 重试（etcd 晚于服务就绪也不致命）；续租中断（etcd 长时间不可达
// 导致租约失效）自动重新注册，直到 ctx 取消。服务端 crash 依赖租约过期清理。
func KeepAlive(ctx context.Context, cli *clientv3.Client, service, addr string, ttl time.Duration) {
	for {
		done, err := Register(ctx, cli, service, addr, ttl)
		if err != nil {
			log.Printf("[etcdreg] register %s/%s: %v", service, addr, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-done:
			// 租约失效：回到循环重新注册。
		}
	}
}

// addrOfKey 从注册 key 的最后一段取实例地址。
func addrOfKey(key string) (string, bool) {
	if i := strings.LastIndexByte(key, '/'); i >= 0 && i+1 < len(key) {
		return key[i+1:], true
	}
	return "", false
}

// List 返回 service 当前的存活地址列表（排序，稳定输出）。
func List(ctx context.Context, cli *clientv3.Client, service string) ([]string, error) {
	return list(ctx, cli, ServicePrefix+service+"/")
}

func list(ctx context.Context, cli *clientv3.Client, prefix string) ([]string, error) {
	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	addrs := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		if a, ok := addrOfKey(string(kv.Key)); ok {
			addrs = append(addrs, a)
		}
	}
	sort.Strings(addrs)
	return addrs, nil
}

// ListAll 返回全部服务的存活地址 map（Ops Console 枚举拓扑的入口，
// 对应原 registry GET /services 无参）。
func ListAll(ctx context.Context, cli *clientv3.Client) (map[string][]string, error) {
	resp, err := cli.Get(ctx, ServicePrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	all := make(map[string][]string)
	for _, kv := range resp.Kvs {
		rest := strings.TrimPrefix(string(kv.Key), ServicePrefix)
		svc, addr, ok := strings.Cut(rest, "/")
		if !ok || svc == "" || addr == "" {
			continue
		}
		all[svc] = append(all[svc], addr)
	}
	for svc := range all {
		sort.Strings(all[svc])
	}
	return all, nil
}

// Watch 先同步回调一次当前列表，此后 service 下每次增删都重读全量并回调
// onUpdate（变即重读，简单可靠）。ctx 取消时返回。
func Watch(ctx context.Context, cli *clientv3.Client, service string, onUpdate func([]string)) {
	prefix := ServicePrefix + service + "/"
	if addrs, err := list(ctx, cli, prefix); err == nil {
		onUpdate(addrs)
	} else {
		log.Printf("[etcdreg] watch %s initial list: %v", service, err)
	}
	wch := cli.Watch(ctx, prefix, clientv3.WithPrefix())
	for range wch {
		if addrs, err := list(ctx, cli, prefix); err == nil {
			onUpdate(addrs)
		}
	}
}
