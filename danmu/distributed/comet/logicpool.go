package main

import (
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/resolver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/YansIlinta/danmu-distributed/etcdreg"
	"github.com/YansIlinta/danmu-distributed/pb"
)

// logicPool 是对 logic 实例集合的一条 gRPC 连接：etcd 官方 naming/resolver 按
// 前缀实时 watch 地址集，round_robin 负载均衡在实例间分发上行弹幕。
//
// 原自研的一致性哈希（roomID→固定 logic 实例）已删除：logic 完全无状态，
// 按房间粘性不是正确性要求，扩缩容也不需要重映射；负载均衡交给 gRPC 的标准策略。
type logicPool struct {
	conn *grpc.ClientConn
	cli  pb.LogicServiceClient
}

// newLogicPool 用 etcd 客户端建连接。target 的路径段是注册 key 前缀
// （见 etcdreg 包注释：无前导斜杠，value 为 naming.Update JSON）。
func newLogicPool(etcdCli *clientv3.Client) (*logicPool, error) {
	builder, err := resolver.NewBuilder(etcdCli)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(
		"etcd:///"+etcdreg.ServicePrefix+"logic",
		grpc.WithResolvers(builder),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}]}`),
	)
	if err != nil {
		return nil, err
	}
	return &logicPool{conn: conn, cli: pb.NewLogicServiceClient(conn)}, nil
}

func (p *logicPool) close() { _ = p.conn.Close() }
