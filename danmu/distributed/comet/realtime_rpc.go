package main

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/YansIlinta/danmu-distributed/core"
	"github.com/YansIlinta/danmu-distributed/pb"
)

// realtimeDeliveryServer is deliberately a thin gRPC adapter. Target semantics
// live in core.MessageEnvelope / Hub.DeliverEnvelope; this layer only validates
// the protobuf representation and translates it to the domain contract.
type realtimeDeliveryServer struct {
	pb.UnimplementedRealtimeDeliveryServiceServer
	hub *core.Hub
}

func registerRealtimeDeliveryService(reg grpc.ServiceRegistrar, hub *core.Hub) {
	pb.RegisterRealtimeDeliveryServiceServer(reg, &realtimeDeliveryServer{hub: hub})
}

func (s *realtimeDeliveryServer) PushEnvelope(ctx context.Context, req *pb.PushEnvelopeReq) (*pb.PushEnvelopeResp, error) {
	if req == nil || req.Envelope == nil {
		return nil, status.Error(codes.InvalidArgument, "envelope is required")
	}
	if s.hub == nil {
		return nil, status.Error(codes.FailedPrecondition, "connection hub is unavailable")
	}

	env, err := coreEnvelopeFromProto(req.Envelope)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	result, err := s.hub.DeliverEnvelope(env, req.ClientPayload)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &pb.PushEnvelopeResp{Delivered: int32(result.Delivered)}, nil
}

func coreEnvelopeFromProto(in *pb.DeliveryEnvelope) (core.MessageEnvelope, error) {
	if in == nil {
		return core.MessageEnvelope{}, fmt.Errorf("envelope is required")
	}

	targetType, err := coreTargetType(in.TargetType)
	if err != nil {
		return core.MessageEnvelope{}, err
	}
	deliveryClass, err := coreDeliveryClass(in.DeliveryClass)
	if err != nil {
		return core.MessageEnvelope{}, err
	}

	env := core.MessageEnvelope{
		MessageID:       in.MessageId,
		ClientMessageID: in.ClientMessageId,
		SenderID:        in.SenderId,
		TargetType:      targetType,
		TargetID:        in.TargetId,
		TargetUserID:    in.TargetUserId,
		DeliveryClass:   deliveryClass,
		MessageType:     core.MessageType(in.MessageType),
		Sequence:        in.Sequence,
		Payload:         json.RawMessage(append([]byte(nil), in.Payload...)),
		ClientTS:        in.ClientTs,
		ServerTS:        in.ServerTs,
	}
	if err := env.Validate(); err != nil {
		return core.MessageEnvelope{}, err
	}
	return env, nil
}

func coreTargetType(in pb.TargetType) (core.TargetType, error) {
	switch in {
	case pb.TargetType_TARGET_USER:
		return core.TargetUser, nil
	case pb.TargetType_TARGET_DEVICE:
		return core.TargetDevice, nil
	case pb.TargetType_TARGET_CHANNEL:
		return core.TargetChannel, nil
	case pb.TargetType_TARGET_BROADCAST:
		return core.TargetBroadcast, nil
	default:
		return "", fmt.Errorf("unsupported target_type %s", in.String())
	}
}

func coreDeliveryClass(in pb.DeliveryClass) (core.DeliveryClass, error) {
	switch in {
	case pb.DeliveryClass_DELIVERY_EPHEMERAL:
		return core.DeliveryEphemeral, nil
	case pb.DeliveryClass_DELIVERY_RELIABLE:
		// This is only a domain/schema value in Phase 2. Durability, sequencing,
		// idempotency, sync and client ACK must be established before callers may
		// make a reliable-delivery claim.
		return core.DeliveryReliable, nil
	default:
		return "", fmt.Errorf("unsupported delivery_class %s", in.String())
	}
}
