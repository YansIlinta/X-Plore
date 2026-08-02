// XClient：面向"服务名"而不是"地址"的客户端。
// 调用方只说 "我要调 Arith 服务"，地址从注册中心来，实例挑选走 round-robin。
// 这是 gRPC 里 Resolver（发现）+ Balancer（选路）两个组件的乞丐版合体。
package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"minirpc/breaker"
	"minirpc/lb"
)

// Discovery 从注册中心拉取某个服务的存活地址，带本地缓存。
// 缓存的意义：不能每次 RPC 都去问一趟注册中心，否则注册中心成了新的单点瓶颈——
// 代价是"实例下线后最多 refresh 时长内还会被选中"，靠调用失败重试兜底。
type Discovery struct {
	registryURL string
	service     string
	refresh     time.Duration

	mu        sync.Mutex
	addrs     []string
	ring      *lb.Ring // 一致性哈希环，与 addrs 同步重建
	idx       int
	lastFetch time.Time
}

func NewDiscovery(registryURL, service string, refresh time.Duration) *Discovery {
	return &Discovery{
		registryURL: registryURL,
		service:     service,
		refresh:     refresh,
		ring:        lb.NewRing(100),
	}
}

// refreshLocked 本地缓存过期时从注册中心拉取地址列表并重建哈希环。
// 调用方必须已持有 d.mu。
func (d *Discovery) refreshLocked() error {
	if time.Since(d.lastFetch) <= d.refresh {
		return nil
	}
	resp, err := http.Get(fmt.Sprintf("%s/services?service=%s", d.registryURL, d.service))
	if err != nil {
		return fmt.Errorf("client: fetch registry: %w", err)
	}
	var addrs []string
	err = json.NewDecoder(resp.Body).Decode(&addrs)
	resp.Body.Close()
	if err != nil {
		return fmt.Errorf("client: decode registry response: %w", err)
	}
	d.addrs = addrs
	d.ring.Reset(addrs)
	d.lastFetch = time.Now()
	return nil
}

// Get 返回下一个实例地址（round-robin）。适合无状态调用，摊匀负载。
func (d *Discovery) Get() (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.refreshLocked(); err != nil {
		return "", err
	}
	if len(d.addrs) == 0 {
		return "", fmt.Errorf("client: no alive instance for service %q", d.service)
	}
	addr := d.addrs[d.idx%len(d.addrs)]
	d.idx++
	return addr, nil
}

// GetKeyed 返回 key 在一致性哈希环上归属的实例。适合有状态路由：
// 同一 key（如弹幕房间号）总落同一实例，实例增删时只有约 1/N 的 key 迁移。
func (d *Discovery) GetKeyed(key string) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.refreshLocked(); err != nil {
		return "", err
	}
	addr := d.ring.Get(key)
	if addr == "" {
		return "", fmt.Errorf("client: no alive instance for service %q", d.service)
	}
	return addr, nil
}

// XClient = Discovery + 按地址缓存的连接池 + 按地址独立的熔断器。
// 熔断器必须按实例（addr）分，不能整个服务共用一个：
// 实例 1 挂了不代表实例 2 也挂了，共用会把健康实例一起误杀。
type XClient struct {
	d *Discovery

	mu       sync.Mutex
	clients  map[string]*Client
	breakers map[string]*breaker.Breaker
}

// 熔断参数：连续 3 次连接级失败 → 熔断 2 秒 → 半开探测。
const (
	breakerThreshold = 3
	breakerCooldown  = 2 * time.Second
)

func NewXClient(d *Discovery) *XClient {
	return &XClient{
		d:        d,
		clients:  make(map[string]*Client),
		breakers: make(map[string]*breaker.Breaker),
	}
}

// Call 无状态调用：round-robin 挑实例，熔断中/连接失败时换下一个重试。
func (x *XClient) Call(ctx context.Context, method string, args, reply any) error {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		addr, err := x.d.Get()
		if err != nil {
			return err
		}
		retryable, err := x.callAddr(ctx, addr, method, args, reply)
		if err == nil || !retryable {
			return err
		}
		lastErr = err
	}
	return lastErr
}

// CallKeyed 有状态调用：key 经一致性哈希总落同一实例（如弹幕房间号 → 固定实例）。
// 注意这里不做"失败换实例"重试——换了实例粘性就破了，那边没有这个 key 的状态；
// 失败直接上抛，降级策略（比如回退到无状态 Call）由调用方按业务决定。
func (x *XClient) CallKeyed(ctx context.Context, key, method string, args, reply any) error {
	addr, err := x.d.GetKeyed(key)
	if err != nil {
		return err
	}
	_, err = x.callAddr(ctx, addr, method, args, reply)
	return err
}

// callAddr 对指定实例完成一次调用：过熔断器 → 取连接 → 调用 → 按错误类型上报。
// retryable 表示"换个实例再试有意义"（熔断中/拨号失败/连接挂了）；
// 超时（ctx 已耗尽）和业务错误不可重试。
func (x *XClient) callAddr(ctx context.Context, addr, method string, args, reply any) (retryable bool, err error) {
	b := x.breakerFor(addr)
	if err := b.Allow(); err != nil {
		return true, fmt.Errorf("%s: %w", addr, err)
	}
	c, err := x.client(addr)
	if err != nil {
		b.Failure() // 拨号失败 = 连接级失败，计入熔断
		return true, err
	}

	err = c.Call(ctx, method, args, reply)
	switch {
	case err == nil:
		b.Success()
		return false, nil
	case errors.Is(err, ErrShutdown):
		// 连接级失败：计入熔断 + 摘掉坏连接
		b.Failure()
		x.dropClient(addr, c)
		return true, err
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		// 超时计入熔断，但 ctx 已耗尽，重试无意义
		b.Failure()
		return false, err
	default:
		// 业务错误：下游能回错误说明它活着，算 Success，不重试
		b.Success()
		return false, err
	}
}

func (x *XClient) breakerFor(addr string) *breaker.Breaker {
	x.mu.Lock()
	defer x.mu.Unlock()
	b := x.breakers[addr]
	if b == nil {
		b = breaker.New(breakerThreshold, breakerCooldown)
		x.breakers[addr] = b
	}
	return b
}

func (x *XClient) dropClient(addr string, c *Client) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.clients[addr] == c {
		delete(x.clients, addr)
	}
}

// client 取缓存的连接，没有就拨号并缓存。
func (x *XClient) client(addr string) (*Client, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if c := x.clients[addr]; c != nil {
		return c, nil
	}
	c, err := Dial(addr)
	if err != nil {
		return nil, err
	}
	x.clients[addr] = c
	return c, nil
}

func (x *XClient) Close() error {
	x.mu.Lock()
	defer x.mu.Unlock()
	for addr, c := range x.clients {
		_ = c.Close()
		delete(x.clients, addr)
	}
	return nil
}
