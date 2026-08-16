package etcdreg

import (
	cryptotls "crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// TLSFiles 是 etcd 客户端可选的 TLS 配置；CAFile 为空 = 明文连接。
// 与 k8s/overlays/etcd-tls 的证书挂载约定一致（/certs/ca.crt 等）。
type TLSFiles struct {
	CAFile   string // 信任的 CA；非空即启用 TLS（RootCAs）
	CertFile string // 客户端证书（双向认证用；为空则仅服务端认证）
	KeyFile  string
}

// TLSFilesFromEnv 从环境变量读 TLS 文件路径（DANMU_ETCD_CA / DANMU_ETCD_CERT /
// DANMU_ETCD_KEY）。全部为空时返回零值 = 明文，因此同一镜像既能跑明文基线
// 也能跑 TLS overlay，无需改任何 flag。
func TLSFilesFromEnv() TLSFiles {
	return TLSFiles{
		CAFile:   os.Getenv("DANMU_ETCD_CA"),
		CertFile: os.Getenv("DANMU_ETCD_CERT"),
		KeyFile:  os.Getenv("DANMU_ETCD_KEY"),
	}
}

// NewClient 构造 etcd 客户端。endpoints 用 https:// 时需 tlsFiles.CAFile 指向服务端 CA。
func NewClient(endpoints []string, tlsFiles TLSFiles) (*clientv3.Client, error) {
	cfg := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 3 * time.Second,
	}
	if tlsFiles.CAFile != "" {
		caPEM, err := os.ReadFile(tlsFiles.CAFile)
		if err != nil {
			return nil, fmt.Errorf("etcd CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("etcd CA: 文件内无有效证书")
		}
		t := &cryptotls.Config{MinVersion: cryptotls.VersionTLS12, RootCAs: pool}
		if tlsFiles.CertFile != "" || tlsFiles.KeyFile != "" {
			cert, err := cryptotls.LoadX509KeyPair(tlsFiles.CertFile, tlsFiles.KeyFile)
			if err != nil {
				return nil, fmt.Errorf("etcd client cert: %w", err)
			}
			t.Certificates = []cryptotls.Certificate{cert}
		}
		cfg.TLS = t
	}
	return clientv3.New(cfg)
}
