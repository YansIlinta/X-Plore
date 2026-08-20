package main

import (
	"context"
	"reflect"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/YansIlinta/danmu-distributed/core"
	"github.com/YansIlinta/danmu-distributed/pb"
)

func TestCoreEnvelopeFromProtoPreservesFields(t *testing.T) {
	in := &pb.DeliveryEnvelope{
		MessageId:       "m1",
		ClientMessageId: "client-1",
		SenderId:        "u-sender",
		TargetType:      pb.TargetType_TARGET_DEVICE,
		TargetId:        "web",
		TargetUserId:    "u-target",
		DeliveryClass:   pb.DeliveryClass_DELIVERY_EPHEMERAL,
		MessageType:     "DANMU",
		Sequence:        42,
		Payload:         []byte(`{"content":"hello"}`),
		ClientTs:        100,
		ServerTs:        200,
	}

	got, err := coreEnvelopeFromProto(in)
	if err != nil {
		t.Fatal(err)
	}
	if got.MessageID != "m1" || got.ClientMessageID != "client-1" || got.SenderID != "u-sender" {
		t.Fatalf("message identity not preserved: %+v", got)
	}
	if got.TargetType != core.TargetDevice || got.TargetID != "web" || got.TargetUserID != "u-target" {
		t.Fatalf("target not preserved: %+v", got)
	}
	if got.DeliveryClass != core.DeliveryEphemeral || got.MessageType != core.MessageDanmu || got.Sequence != 42 {
		t.Fatalf("message semantics not preserved: %+v", got)
	}
	if !reflect.DeepEqual([]byte(got.Payload), in.Payload) || got.ClientTS != 100 || got.ServerTS != 200 {
		t.Fatalf("payload/timestamps not preserved: %+v", got)
	}

	// Ensure conversion copied the bytes instead of aliasing protobuf storage.
	in.Payload[0] = 'X'
	if got.Payload[0] == 'X' {
		t.Fatal("domain envelope payload must not alias protobuf input bytes")
	}
}

func TestCoreEnvelopeFromProtoRejectsUnspecifiedEnums(t *testing.T) {
	base := &pb.DeliveryEnvelope{
		TargetType:    pb.TargetType_TARGET_USER,
		TargetId:      "u1",
		DeliveryClass: pb.DeliveryClass_DELIVERY_EPHEMERAL,
		MessageType:   "DANMU",
	}

	m := *base
	m.TargetType = pb.TargetType_TARGET_TYPE_UNSPECIFIED
	if _, err := coreEnvelopeFromProto(&m); err == nil {
		t.Fatal("unspecified target type must fail")
	}

	m = *base
	m.DeliveryClass = pb.DeliveryClass_DELIVERY_CLASS_UNSPECIFIED
	if _, err := coreEnvelopeFromProto(&m); err == nil {
		t.Fatal("unspecified delivery class must fail")
	}
}

func TestPushEnvelopeRejectsMissingEnvelope(t *testing.T) {
	s := &realtimeDeliveryServer{hub: core.NewHub("gw-test", context.Background())}
	_, err := s.PushEnvelope(context.Background(), &pb.PushEnvelopeReq{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status=%v err=%v want InvalidArgument", status.Code(err), err)
	}
}

func TestPushEnvelopeDispatchesValidRequest(t *testing.T) {
	s := &realtimeDeliveryServer{hub: core.NewHub("gw-test", context.Background())}
	resp, err := s.PushEnvelope(context.Background(), &pb.PushEnvelopeReq{
		Envelope: &pb.DeliveryEnvelope{
			TargetType:    pb.TargetType_TARGET_USER,
			TargetId:      "offline-user",
			DeliveryClass: pb.DeliveryClass_DELIVERY_EPHEMERAL,
			MessageType:   "DANMU",
		},
		ClientPayload: []byte(`[{"type":"danmu"}]`),
	})
	if err != nil {
		t.Fatalf("PushEnvelope valid request: %v", err)
	}
	if resp.Delivered != 0 {
		t.Fatalf("Delivered=%d want 0 for user with no local connection", resp.Delivered)
	}
}

func TestPushEnvelopeDoesNotInventReliableGuarantee(t *testing.T) {
	s := &realtimeDeliveryServer{hub: core.NewHub("gw-test", context.Background())}
	resp, err := s.PushEnvelope(context.Background(), &pb.PushEnvelopeReq{
		Envelope: &pb.DeliveryEnvelope{
			TargetType:    pb.TargetType_TARGET_USER,
			TargetId:      "offline-user",
			DeliveryClass: pb.DeliveryClass_DELIVERY_RELIABLE,
			MessageType:   "DANMU",
		},
		ClientPayload: []byte(`[{"type":"danmu"}]`),
	})
	if err != nil {
		t.Fatalf("RELIABLE enum is a valid Phase-2 domain value: %v", err)
	}
	if resp.Delivered != 0 {
		t.Fatalf("Delivered=%d want 0", resp.Delivered)
	}
}
