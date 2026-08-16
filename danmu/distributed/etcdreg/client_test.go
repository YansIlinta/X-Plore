package etcdreg

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/transport"
	"go.etcd.io/etcd/server/v3/embed"
)

// genTLS 生成自签 CA 与 127.0.0.1 服务端证书（也带 ClientAuth EKU，便于复用为客户端证书），
// 写入 dir 下的 PEM 文件并返回路径。
func genTLS(t *testing.T, dir string) (caFile, certFile, keyFile string) {
	t.Helper()

	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "danmu-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("ca cert: %v", err)
	}

	srvKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("srv key: %v", err)
	}
	srvTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTmpl, caTmpl, &srvKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("srv cert: %v", err)
	}

	caFile = filepath.Join(dir, "ca.crt")
	certFile = filepath.Join(dir, "server.crt")
	keyFile = filepath.Join(dir, "server.key")
	writePEM(t, caFile, "CERTIFICATE", caDER)
	writePEM(t, certFile, "CERTIFICATE", srvDER)
	writePEM(t, keyFile, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(srvKey))
	return
}

func writePEM(t *testing.T, path, blockType string, der []byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	defer f.Close()
	if err := pem.Encode(f, &pem.Block{Type: blockType, Bytes: der}); err != nil {
		t.Fatalf("pem %s: %v", path, err)
	}
}

// startEmbedEtcdTLS 起一个 https 客户端端口的单节点 embed etcd。
func startEmbedEtcdTLS(t *testing.T, tlsFiles TLSFiles) (string, func()) {
	t.Helper()
	clientPort := freePort(t)
	peerPort := freePort(t)
	clientURL, _ := url.Parse(fmt.Sprintf("https://127.0.0.1:%d", clientPort))
	peerURL, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", peerPort))

	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.Name = "testetcdtls"
	cfg.ListenClientUrls = []url.URL{*clientURL}
	cfg.AdvertiseClientUrls = []url.URL{*clientURL}
	cfg.ListenPeerUrls = []url.URL{*peerURL}
	cfg.AdvertisePeerUrls = []url.URL{*peerURL}
	cfg.InitialCluster = cfg.Name + "=" + peerURL.String()
	cfg.LogLevel = "fatal"
	cfg.ClientTLSInfo = transport.TLSInfo{
		TrustedCAFile: tlsFiles.CAFile,
		CertFile:      tlsFiles.CertFile,
		KeyFile:       tlsFiles.KeyFile,
	}

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatalf("embed etcd tls: %v", err)
	}
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(15 * time.Second):
		e.Close()
		t.Fatalf("etcd(tls) 15s 内未就绪")
	}
	return clientURL.String(), func() { e.Close() }
}

// NewClient 的 TLS 路径：https 端点 + 自签 CA + 客户端证书，打通注册/发现往返；
// 无有效证书的 CA 文件必须在构造期报错。
func TestNewClientTLS(t *testing.T) {
	dir := t.TempDir()
	caFile, certFile, keyFile := genTLS(t, dir)
	httpsURL, cleanup := startEmbedEtcdTLS(t, TLSFiles{CAFile: caFile, CertFile: certFile, KeyFile: keyFile})
	defer cleanup()

	cli, err := NewClient([]string{httpsURL}, TLSFiles{CAFile: caFile, CertFile: certFile, KeyFile: keyFile})
	if err != nil {
		t.Fatalf("NewClient(tls): %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := Register(ctx, cli, "logic", "localhost:7400", 10*time.Second); err != nil {
		t.Fatalf("register(tls): %v", err)
	}
	addrs, err := List(ctx, cli, "logic")
	if err != nil || len(addrs) != 1 || addrs[0] != "localhost:7400" {
		t.Fatalf("list(tls): addrs=%v err=%v", addrs, err)
	}

	// 垃圾 CA 文件 → 构造期失败，而不是运行时才炸。
	bad := TLSFiles{CAFile: filepath.Join(dir, "bad-ca.crt")}
	if err := os.WriteFile(bad.CAFile, []byte("not a pem"), 0o644); err != nil {
		t.Fatalf("write bad ca: %v", err)
	}
	if _, err := NewClient([]string{httpsURL}, bad); err == nil {
		t.Fatalf("垃圾 CA 不应构造成功")
	}

	// 有效但不匹配的 CA → 握手失败（List 报错），证明 RootCAs 生效。
	wrongDir := t.TempDir()
	wrongCA, _, _ := genTLS(t, wrongDir)
	wrong, err := NewClient([]string{httpsURL}, TLSFiles{CAFile: wrongCA})
	if err != nil {
		t.Fatalf("NewClient(wrong ca): %v", err)
	}
	defer wrong.Close()
	if _, err := List(ctx, wrong, "logic"); err == nil {
		t.Fatalf("不匹配 CA 的 List 不应成功")
	}
}

// TLSFilesFromEnv 的环境变量映射。
func TestTLSFilesFromEnv(t *testing.T) {
	t.Setenv("DANMU_ETCD_CA", "/certs/ca.crt")
	t.Setenv("DANMU_ETCD_CERT", "/certs/client.crt")
	t.Setenv("DANMU_ETCD_KEY", "/certs/client.key")
	got := TLSFilesFromEnv()
	if got.CAFile != "/certs/ca.crt" || got.CertFile != "/certs/client.crt" || got.KeyFile != "/certs/client.key" {
		t.Fatalf("TLSFilesFromEnv=%+v", got)
	}

	t.Setenv("DANMU_ETCD_CA", "")
	t.Setenv("DANMU_ETCD_CERT", "")
	t.Setenv("DANMU_ETCD_KEY", "")
	if got := TLSFilesFromEnv(); got != (TLSFiles{}) {
		t.Fatalf("空环境应得到零值: %+v", got)
	}
}
