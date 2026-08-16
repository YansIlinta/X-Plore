// chaintest 是 goim 链路的客户端侧集成验证（不依赖 Kafka）：
//  1. etcd 服务发现：comet 已注册，能查到其 gRPC 地址
//  2. Logic.OnMessage：敏感词过滤 + 生成 msg_id（直连 logic gRPC）
//  3. Job→Comet.PushRoom→WS：模拟 job 调 comet.PushRoom，验证 WS 客户端收到广播
//  4. 上行：WS 发弹幕，comet danmu_messages_total{in} 递增（readPump→uplink 生效）
//
// 前置：一个 etcd（默认 localhost:17379），以及用 -etcd 指到它、已启动的 logic+comet。
// Kafka 段（logic→Kafka→job 消费）为标准 kafka-go，无 broker 时不在此验证。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/YansIlinta/danmu-distributed/core"
	"github.com/YansIlinta/danmu-distributed/etcdreg"
	"github.com/YansIlinta/danmu-distributed/pb"
)

func main() {
	cometWS := flag.String("comet-ws", "localhost:18080", "comet WS/HTTP addr")
	cometRPC := flag.String("comet-rpc", "localhost:17500", "comet gRPC addr")
	logicRPC := flag.String("logic-rpc", "localhost:17400", "logic gRPC addr")
	etcdEndpoints := flag.String("etcd", "localhost:17379", "etcd 客户端端点(逗号分隔)")
	token := flag.String("token", "danmu-secret-token", "auth token")
	flag.Parse()

	fails := 0
	step := func(name string, fn func() error) {
		if err := fn(); err != nil {
			fmt.Printf("  ✗ %s: %v\n", name, err)
			fails++
		} else {
			fmt.Printf("  ✓ %s\n", name)
		}
	}

	fmt.Println("=== goim 链路集成验证 ===")

	step("etcd 发现 comet", func() error {
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   strings.Split(*etcdEndpoints, ","),
			DialTimeout: 3 * time.Second,
		})
		if err != nil {
			return err
		}
		defer cli.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		addrs, err := etcdreg.List(ctx, cli, "comet")
		if err != nil {
			return err
		}
		if len(addrs) == 0 {
			return fmt.Errorf("etcd 未发现 comet 实例")
		}
		fmt.Printf("    comet 实例: %v\n", addrs)
		return nil
	})

	step("Logic.OnMessage 过滤+msg_id", func() error {
		conn, err := grpc.NewClient(*logicRPC, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer conn.Close()
		cli := pb.NewLogicServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		resp, err := cli.OnMessage(ctx, &pb.OnMessageReq{
			RoomId: "room-x", Uid: "u1", Content: "正常内容办假证结尾",
			ClientTs: time.Now().UnixMilli(), SourceComet: "chaintest",
		})
		if err != nil {
			// 无 Kafka broker 时 produce 失败属预期：gRPC 已到达 logic 并处理到写 Kafka 前。
			if strings.Contains(err.Error(), "kafka") {
				fmt.Printf("    (gRPC 到达 logic；因无 Kafka broker 未能 produce——预期。过滤/msg_id 由 core 单测覆盖)\n")
				return nil
			}
			return err
		}
		if resp.MsgId == "" {
			return fmt.Errorf("msg_id 为空")
		}
		if !strings.Contains(resp.Filtered, "*") {
			return fmt.Errorf("敏感词未打码: %q", resp.Filtered)
		}
		fmt.Printf("    msg_id=%s filtered=%q\n", resp.MsgId, resp.Filtered)
		return nil
	})

	step("Job→Comet.PushRoom→WS 客户端收到广播", func() error {
		room := "chain-room"
		// 1) 先建 WS 连接并进房
		wsURL := fmt.Sprintf("ws://%s/ws?uid=wsclient&room=%s&token=%s", *cometWS, room, *token)
		ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			return fmt.Errorf("ws dial: %w", err)
		}
		defer ws.Close()
		time.Sleep(300 * time.Millisecond) // 等 AddClient 完成

		recv := make(chan string, 8)
		go func() {
			for {
				_, data, err := ws.ReadMessage()
				if err != nil {
					return
				}
				recv <- string(data)
			}
		}()

		// 2) 模拟 job 调 comet.PushRoom
		conn, err := grpc.NewClient(*cometRPC, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer conn.Close()
		cli := pb.NewCometServiceClient(conn)
		payload := []byte(`[{"type":"danmu","msg_id":"m-1","room_id":"chain-room","uid":"sysu","content":"hello-from-job"}]`)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		resp, err := cli.PushRoom(ctx, &pb.PushRoomReq{RoomId: room, Payload: payload})
		if err != nil {
			return fmt.Errorf("PushRoom: %w", err)
		}
		if resp.Delivered < 1 {
			return fmt.Errorf("PushRoom delivered=%d，期望≥1", resp.Delivered)
		}

		// 3) WS 客户端应收到该广播（跳过连上时下发的 session_token 等控制消息）
		deadline := time.After(3 * time.Second)
		for {
			select {
			case msg := <-recv:
				if strings.Contains(msg, "session_token") || strings.Contains(msg, "reauth_ack") || strings.Contains(msg, "rate_limited") {
					continue // 控制消息，跳过
				}
				if !strings.Contains(msg, "hello-from-job") {
					return fmt.Errorf("收到的不是期望消息: %s", msg)
				}
				fmt.Printf("    delivered=%d, WS 收到: %s\n", resp.Delivered, msg)
				return nil
			case <-deadline:
				return fmt.Errorf("WS 客户端 3s 内未收到 PushRoom 广播")
			}
		}
	})

	step("上行：WS 发弹幕后 comet messages_in 递增", func() error {
		before := scrapeCounter(*cometWS, `danmu_messages_total{direction="in"}`)
		wsURL := fmt.Sprintf("ws://%s/ws?uid=up1&room=up-room&token=%s", *cometWS, *token)
		ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			return err
		}
		defer ws.Close()
		time.Sleep(200 * time.Millisecond)
		for i := 0; i < 5; i++ {
			ws.WriteMessage(websocket.TextMessage, []byte(`{"type":"danmu","content":"hi","client_ts":1}`))
		}
		time.Sleep(500 * time.Millisecond)
		after := scrapeCounter(*cometWS, `danmu_messages_total{direction="in"}`)
		if after <= before {
			return fmt.Errorf("messages_in 未递增 (before=%.0f after=%.0f)", before, after)
		}
		fmt.Printf("    messages_in: %.0f → %.0f\n", before, after)
		return nil
	})

	step("PushRoom 带 trace metadata → comet 记录 comet.deliver span", func() error {
		room := "trace-room"
		// 1) 建 WS 连接进房，保证 PushRoom 有投递对象
		wsURL := fmt.Sprintf("ws://%s/ws?uid=traceclient&room=%s&token=%s", *cometWS, room, *token)
		ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			return fmt.Errorf("ws dial: %w", err)
		}
		defer ws.Close()
		time.Sleep(300 * time.Millisecond) // 等 AddClient 完成

		// 2) 模拟 job：带 danmu-trace-msgids metadata 调 PushRoom
		conn, err := grpc.NewClient(*cometRPC, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer conn.Close()
		cli := pb.NewCometServiceClient(conn)
		traceID := "trace-smoke-1"
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		ctx = metadata.AppendToOutgoingContext(ctx, core.TraceMetadataKey, traceID)
		payload := []byte(`[{"type":"danmu","msg_id":"` + traceID + `","room_id":"` + room + `","uid":"sysu","content":"trace"}]`)
		resp, err := cli.PushRoom(ctx, &pb.PushRoomReq{RoomId: room, Payload: payload})
		if err != nil {
			return fmt.Errorf("PushRoom: %w", err)
		}
		if resp.Delivered < 1 {
			return fmt.Errorf("PushRoom delivered=%d，期望≥1", resp.Delivered)
		}

		// 3) 轮询 comet /api/v1/traces，该 msg_id 应出现 comet.deliver span
		deadline := time.After(3 * time.Second)
		for {
			select {
			case <-deadline:
				return fmt.Errorf("3s 内 comet 未记录 %s 的 %s span", traceID, core.HopCometDeliver)
			default:
			}
			spans, err := fetchTraceSpans(*cometWS, *token)
			if err != nil {
				return err
			}
			for _, sp := range spans {
				if sp.MsgID == traceID && sp.Hop == core.HopCometDeliver {
					fmt.Printf("    %s span: node=%s detail=%q\n", traceID, sp.Node, sp.Detail)
					return nil
				}
			}
			time.Sleep(200 * time.Millisecond)
		}
	})

	fmt.Println()
	if fails > 0 {
		fmt.Printf("FAILED: %d 项未通过\n", fails)
		os.Exit(1)
	}
	fmt.Println("ALL PASSED ✓")
}

// traceSpan 是 /api/v1/traces 返回的 span 中 chaintest 关心的字段。
type traceSpan struct {
	MsgID  string `json:"msg_id"`
	Hop    string `json:"hop"`
	Node   string `json:"node"`
	Detail string `json:"detail"`
}

// fetchTraceSpans 拉取 comet 的 span 列表（观测端点要求 Bearer token）。
func fetchTraceSpans(hostPort, token string) ([]traceSpan, error) {
	req, err := http.NewRequest(http.MethodGet, "http://"+hostPort+"/api/v1/traces", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var body struct {
		Spans []traceSpan `json:"spans"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, err
	}
	return body.Spans, nil
}

// scrapeCounter 从 /metrics 抓一个精确匹配的计数器值。
func scrapeCounter(hostPort, name string) float64 {
	resp, err := http.Get("http://" + hostPort + "/metrics")
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, name+" ") {
			var v float64
			fmt.Sscanf(strings.TrimPrefix(line, name+" "), "%g", &v)
			return v
		}
	}
	return 0
}
