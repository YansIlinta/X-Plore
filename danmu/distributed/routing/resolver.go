package routing

import (
	"context"
	"errors"
	"sort"

	"github.com/YansIlinta/danmu-distributed/core"
)

var ErrGatewayDirectoryRequired = errors.New("gateway directory is required for broadcast target")

// GatewayDirectory lists currently live Gateway instances. It is intentionally
// separate from Store: service discovery (etcd today) and dynamic user/channel
// routing (Redis later) have different lifecycles and should not share storage.
type GatewayDirectory interface {
	ListGateways(ctx context.Context) ([]string, error)
}

// TargetResolver maps a domain MessageEnvelope to the minimal Gateway set that
// should receive the local-delivery RPC. USER/DEVICE/CHANNEL use RouteStore;
// explicit BROADCAST uses service discovery.
type TargetResolver struct {
	Store     Store
	Gateways  GatewayDirectory
}

func (r TargetResolver) Resolve(ctx context.Context, env core.MessageEnvelope) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := env.Validate(); err != nil {
		return nil, err
	}
	if r.Store == nil && env.TargetType != core.TargetBroadcast {
		return nil, errors.New("route store is required")
	}

	var (
		gateways []string
		err      error
	)
	switch env.TargetType {
	case core.TargetUser:
		gateways, err = r.Store.LookupUser(ctx, env.TargetID)
	case core.TargetDevice:
		gateways, err = r.Store.LookupDevice(ctx, env.TargetUserID, env.TargetID)
	case core.TargetChannel:
		gateways, err = r.Store.LookupChannel(ctx, env.TargetID)
	case core.TargetBroadcast:
		if r.Gateways == nil {
			return nil, ErrGatewayDirectoryRequired
		}
		gateways, err = r.Gateways.ListGateways(ctx)
	default:
		// MessageEnvelope.Validate currently rejects this; keep defensive parity
		// with Hub.DeliverEnvelope for future enum additions.
		return nil, errors.New("unsupported target type")
	}
	if err != nil {
		return nil, err
	}
	return normalizeGateways(gateways), nil
}

func normalizeGateways(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, gatewayID := range in {
		if gatewayID == "" {
			continue
		}
		if _, ok := seen[gatewayID]; ok {
			continue
		}
		seen[gatewayID] = struct{}{}
		out = append(out, gatewayID)
	}
	sort.Strings(out)
	return out
}
