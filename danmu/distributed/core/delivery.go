package core

import "fmt"

// LocalDeliveryResult describes a delivery performed against one Comet's local
// connection kernel. Cross-Gateway routing is intentionally outside this type:
// Phase 3 will resolve the target Gateway(s) first, then invoke this same local
// dispatch contract on each selected Gateway.
type LocalDeliveryResult struct {
	Delivered int
}

// List returns a point-in-time snapshot of all local connections. It is used
// only for explicit BROADCAST delivery; normal USER/DEVICE/CHANNEL delivery
// stays index-based and does not scan all connections.
func (m *ConnectionManager) List() []*Client {
	var out []*Client
	for _, sh := range m.shards {
		sh.mu.RLock()
		for _, c := range sh.conns {
			out = append(out, c)
		}
		sh.mu.RUnlock()
	}
	return out
}

// DeliverEnvelope dispatches an already-serialized client payload according to
// the generic MessageEnvelope target against this Hub only.
//
// It deliberately accepts payload separately from env.Payload: MessageEnvelope
// is the routing/domain contract, while the WebSocket wire representation may
// remain legacy Danmu JSON during migration. This keeps Phase 2 compatible with
// the current clients and lets later protocol versions choose their own wire
// encoding without changing target semantics.
//
// DeliveryReliable is accepted by Validate as a domain value but this function
// does NOT make it reliable. Durability/idempotency/sequence/offline sync are
// Phase 4 concerns and must happen before this local delivery step.
func (h *Hub) DeliverEnvelope(env MessageEnvelope, payload []byte) (LocalDeliveryResult, error) {
	if err := env.Validate(); err != nil {
		return LocalDeliveryResult{}, err
	}
	if len(payload) == 0 {
		return LocalDeliveryResult{}, fmt.Errorf("delivery payload is empty")
	}

	var delivered int
	switch env.TargetType {
	case TargetUser:
		delivered = h.PushUser(env.TargetID, payload)
	case TargetDevice:
		delivered = h.PushDevice(env.TargetUserID, env.TargetID, payload)
	case TargetChannel:
		delivered = h.subs.PushChannel(env.TargetID, payload)
		if delivered > 0 {
			MetricMsgOut(delivered)
		}
	case TargetBroadcast:
		dropped := 0
		for _, c := range h.connMan.List() {
			if c.TrySend(payload) {
				delivered++
			} else {
				dropped++
			}
		}
		if dropped > 0 {
			MetricDropped(dropped)
		}
		if delivered > 0 {
			MetricMsgOut(delivered)
		}
	default:
		// Validate rejects this path; keep the switch defensive for future enum
		// additions that forget to update local delivery.
		return LocalDeliveryResult{}, fmt.Errorf("unsupported target type %q", env.TargetType)
	}

	return LocalDeliveryResult{Delivered: delivered}, nil
}
