package routing

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/YansIlinta/danmu-distributed/core"
)

type stubGatewayDirectory struct {
	gateways []string
	err      error
}

func (s stubGatewayDirectory) ListGateways(context.Context) ([]string, error) {
	return s.gateways, s.err
}

func validEnvelope(tt core.TargetType) core.MessageEnvelope {
	return core.MessageEnvelope{
		TargetType:    tt,
		DeliveryClass: core.DeliveryEphemeral,
		MessageType:   core.MessageDanmu,
	}
}

func TestTargetResolverUserDeviceChannel(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	ttl := time.Minute

	routes := []ConnectionRoute{
		{ConnectionID: "c1", UserID: "u1", DeviceID: "web", GatewayID: "gw-b", ChannelIDs: []string{"live:1"}},
		{ConnectionID: "c2", UserID: "u1", DeviceID: "mobile", GatewayID: "gw-a", ChannelIDs: []string{"live:1"}},
		{ConnectionID: "c3", UserID: "u2", DeviceID: "web", GatewayID: "gw-c", ChannelIDs: []string{"live:2"}},
	}
	for _, route := range routes {
		if err := store.UpsertConnection(ctx, route, ttl); err != nil {
			t.Fatal(err)
		}
	}

	resolver := TargetResolver{Store: store}

	user := validEnvelope(core.TargetUser)
	user.TargetID = "u1"
	if got, err := resolver.Resolve(ctx, user); err != nil || !reflect.DeepEqual(got, []string{"gw-a", "gw-b"}) {
		t.Fatalf("user resolve=%v err=%v", got, err)
	}

	device := validEnvelope(core.TargetDevice)
	device.TargetUserID = "u1"
	device.TargetID = "web"
	if got, err := resolver.Resolve(ctx, device); err != nil || !reflect.DeepEqual(got, []string{"gw-b"}) {
		t.Fatalf("device resolve=%v err=%v", got, err)
	}

	channel := validEnvelope(core.TargetChannel)
	channel.TargetID = "live:1"
	if got, err := resolver.Resolve(ctx, channel); err != nil || !reflect.DeepEqual(got, []string{"gw-a", "gw-b"}) {
		t.Fatalf("channel resolve=%v err=%v", got, err)
	}
}

func TestTargetResolverBroadcastUsesGatewayDirectory(t *testing.T) {
	ctx := context.Background()
	resolver := TargetResolver{
		Store: NewMemoryStore(),
		Gateways: stubGatewayDirectory{gateways: []string{"gw-b", "gw-a", "gw-b", ""}},
	}

	env := validEnvelope(core.TargetBroadcast)
	got, err := resolver.Resolve(ctx, env)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []string{"gw-a", "gw-b"}) {
		t.Fatalf("broadcast resolve=%v want [gw-a gw-b]", got)
	}
}

func TestTargetResolverBroadcastRequiresDirectory(t *testing.T) {
	env := validEnvelope(core.TargetBroadcast)
	_, err := (TargetResolver{Store: NewMemoryStore()}).Resolve(context.Background(), env)
	if !errors.Is(err, ErrGatewayDirectoryRequired) {
		t.Fatalf("error=%v want ErrGatewayDirectoryRequired", err)
	}
}

func TestTargetResolverRejectsMissingStoreForIndexedTarget(t *testing.T) {
	env := validEnvelope(core.TargetUser)
	env.TargetID = "u1"
	_, err := (TargetResolver{}).Resolve(context.Background(), env)
	if err == nil {
		t.Fatal("missing Store should fail indexed target resolution")
	}
}

func TestTargetResolverPropagatesDirectoryError(t *testing.T) {
	want := errors.New("discovery unavailable")
	env := validEnvelope(core.TargetBroadcast)
	_, err := (TargetResolver{Gateways: stubGatewayDirectory{err: want}}).Resolve(context.Background(), env)
	if !errors.Is(err, want) {
		t.Fatalf("error=%v want %v", err, want)
	}
}
