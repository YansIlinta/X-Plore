package ops

import (
	"path/filepath"
	"strings"
	"testing"
)

func evidenceStore(t *testing.T) *experimentStore {
	t.Helper()
	s, err := NewExperimentStore(filepath.Join(t.TempDir(), "data"), 200)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func saveExp(t *testing.T, s *experimentStore, exp *Experiment) {
	t.Helper()
	if err := s.Save(exp); err != nil {
		t.Fatal(err)
	}
}

func claimsByID(t *testing.T, s *experimentStore) map[string]*Claim {
	t.Helper()
	ev := NewEvidenceService(s, "")
	all := ev.List()
	byID := map[string]*Claim{}
	for i := range all {
		c := all[i]
		byID[c.ID] = &c
	}
	return byID
}

func (c *Claim) assertStatus(t *testing.T, want ClaimStatus) {
	t.Helper()
	if c.Status != want {
		t.Fatalf("claim %s: status=%s, want %s", c.ID, c.Status, want)
	}
}

func TestEvidenceEmptyStore(t *testing.T) {
	s := evidenceStore(t)
	byID := claimsByID(t, s)
	// 无实验：benchmark / 目标型 claim 必须是 TARGET，绝不冒充 VERIFIED
	byID["claim-10k-connections"].assertStatus(t, ClaimTarget)
	byID["claim-million-connections"].assertStatus(t, ClaimTarget)
	byID["claim-hot-room-observed"].assertStatus(t, ClaimTarget)
	// 代码型 claim 是 CODE VERIFIED（代码/测试层面验证，不是 benchmark）
	byID["claim-etcd-service-discovery"].assertStatus(t, ClaimCodeVerified)
	byID["claim-hub-256-shard"].assertStatus(t, ClaimCodeVerified)
	byID["claim-kafka-fanout-chain"].assertStatus(t, ClaimCodeVerified)
	byID["claim-monolith-local-broadcast"].assertStatus(t, ClaimCodeVerified)
	byID["claim-reproducible-meta"].assertStatus(t, ClaimCodeVerified)
	if byID["claim-hub-256-shard"].Date == nil {
		t.Fatalf("code claim should carry a date")
	}
}

func TestEvidenceVerifiedAt10k(t *testing.T) {
	s := evidenceStore(t)
	exp := mkExp("exp-10k", ArchMonolith, func(r *ExperimentResult) { r.ConnectionsEstablished = intp(12000) })
	saveExp(t, s, exp)
	byID := claimsByID(t, s)
	byID["claim-10k-connections"].assertStatus(t, ClaimVerified)
	if byID["claim-10k-connections"].ExperimentID == nil || *byID["claim-10k-connections"].ExperimentID != "exp-10k" {
		t.Fatalf("10k claim evidence experiment not linked: %+v", byID["claim-10k-connections"])
	}
	if byID["claim-10k-connections"].Environment == nil {
		t.Fatalf("experiment environment must carry over to claim")
	}
	// 百万仍是目标，不会因为 10k 而自动升级
	byID["claim-million-connections"].assertStatus(t, ClaimTarget)
}

func TestEvidencePartiallyVerified(t *testing.T) {
	s := evidenceStore(t)
	exp := mkExp("exp-5k", ArchMonolith, func(r *ExperimentResult) { r.ConnectionsEstablished = intp(5000) })
	saveExp(t, s, exp)
	byID := claimsByID(t, s)
	c := byID["claim-10k-connections"]
	c.assertStatus(t, ClaimPartially)
	if !containsStr(c.Evidence, "exp-5k") || !strings.Contains(c.Notes, "5000") {
		t.Fatalf("partial claim should point at highest experiment and its value: %+v", c)
	}
}

func TestEvidenceKafkaUpgrade(t *testing.T) {
	s := evidenceStore(t)
	exp := mkExp("exp-kafka", ArchDistributed, func(r *ExperimentResult) {
		r.KafkaAvailable = newbool(true)
		r.ServiceSnapshot = &DistributedSnapshot{CometTotal: 2, CometHealthy: 2, LogicTotal: 1, JobTotal: 1, EtcdUp: true}
	})
	saveExp(t, s, exp)
	byID := claimsByID(t, s)
	c := byID["claim-kafka-fanout-chain"]
	c.assertStatus(t, ClaimVerified) // 真 broker 分布式实验 → 升级
	if c.ExperimentID == nil || *c.ExperimentID != "exp-kafka" {
		t.Fatalf("kafka claim evidence not linked: %+v", c)
	}
}

func TestEvidenceHotRoomCapability(t *testing.T) {
	s := evidenceStore(t)
	exp := mkExp("exp-hot", ArchMonolith, func(r *ExperimentResult) { r.P90LatencyUS = intp(1500) })
	exp.Preset = "hot-room"
	saveExp(t, s, exp)
	byID := claimsByID(t, s)
	byID["claim-hot-room-observed"].assertStatus(t, ClaimVerified)
}

func containsStr(ss []string, sub string) bool {
	for _, s := range ss {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}
