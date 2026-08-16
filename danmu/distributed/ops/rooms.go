package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
)

// 房间聚合：ops 本身不存房间数据，每次请求向各健康 comet 的 /api/v1/rooms 扇出查询
// 并在内存里合并。comet 侧是全量枚举 + 分页（limit≤100），房间数大时这是有成本的——
// 所以该端点是"按需查询"（打开 Rooms 页才调），不进入周期采集循环。

// RoomView 是跨 comet 合并后的房间视图。
type RoomView struct {
	RoomID      string   `json:"room_id"`
	OnlineCount int      `json:"online_count"`
	Comets      []string `json:"comets"` // 持有该房间连接的 comet 实例
	Active      bool     `json:"is_active"`
}

type cometRoomsResp struct {
	Items []struct {
		RoomID      string `json:"room_id"`
		OnlineCount int    `json:"online_count"`
		IsActive    bool   `json:"is_active"`
	} `json:"items"`
}

// fetchRooms 拉单台 comet 的房间列表（第一页，最多 100 条）。
func (c *Collector) fetchRooms(ctx context.Context, httpAddr string) (*cometRoomsResp, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://"+httpAddr+"/api/v1/rooms?page=1&limit=100", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.Token)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	var body cometRoomsResp
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, err
	}
	return &body, nil
}

// aggregateRooms 向所有健康 comet 扇出查询并按 room_id 合并；返回合并结果与失败实例数。
func (c *Collector) aggregateRooms(ctx context.Context, roomFilter string) ([]RoomView, int) {
	snap := c.Snapshot()
	var comets []string
	for _, svc := range snap.Services {
		if svc.Name != "comet" {
			continue
		}
		for _, it := range svc.Instances {
			if it.Healthy {
				comets = append(comets, it.HTTPAddr)
			}
		}
	}

	type result struct {
		addr string
		resp *cometRoomsResp
		err  error
	}
	resCh := make(chan result, len(comets))
	var wg sync.WaitGroup
	for _, addr := range comets {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			resp, err := c.fetchRooms(ctx, addr)
			resCh <- result{addr, resp, err}
		}(addr)
	}
	wg.Wait()
	close(resCh)

	failed := 0
	merged := map[string]*RoomView{}
	for r := range resCh {
		if r.err != nil {
			failed++
			continue
		}
		for _, item := range r.resp.Items {
			if roomFilter != "" && item.RoomID != roomFilter {
				continue
			}
			rv := merged[item.RoomID]
			if rv == nil {
				rv = &RoomView{RoomID: item.RoomID}
				merged[item.RoomID] = rv
			}
			rv.OnlineCount += item.OnlineCount
			rv.Comets = append(rv.Comets, r.addr)
			rv.Active = rv.Active || item.IsActive
		}
	}

	out := make([]RoomView, 0, len(merged))
	for _, rv := range merged {
		sort.Strings(rv.Comets)
		out = append(out, *rv)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].OnlineCount > out[j].OnlineCount })
	return out, failed
}
