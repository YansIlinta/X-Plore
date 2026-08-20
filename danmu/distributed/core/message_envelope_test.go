package core

import (
	"encoding/json"
	"testing"
)

func TestDanmuEnvelopeCompatibility(t *testing.T) {
	env := NewDanmuEnvelope("room-42", "user-7", "hello", 123, 456)

	if env.TargetType != TargetChannel {
		t.Fatalf("TargetType=%q want %q", env.TargetType, TargetChannel)
	}
	if env.TargetID != "danmu:room:room-42" {
		t.Fatalf("TargetID=%q want danmu:room:room-42", env.TargetID)
	}
	if env.DeliveryClass != DeliveryEphemeral {
		t.Fatalf("DeliveryClass=%q want %q", env.DeliveryClass, DeliveryEphemeral)
	}
	if env.MessageType != MessageDanmu {
		t.Fatalf("MessageType=%q want %q", env.MessageType, MessageDanmu)
	}
	if env.SenderID != "user-7" || env.ClientTS != 123 || env.ServerTS != 456 {
		t.Fatalf("identity/timestamps not preserved: %+v", env)
	}

	var payload struct {
		Content string `json:"content"`
	}
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		t.Fatalf("payload unmarshal: %v", err)
	}
	if payload.Content != "hello" {
		t.Fatalf("payload content=%q want hello", payload.Content)
	}
	if err := env.Validate(); err != nil {
		t.Fatalf("compat envelope should validate: %v", err)
	}
}

func TestMessageEnvelopeValidateTargets(t *testing.T) {
	base := MessageEnvelope{
		DeliveryClass: DeliveryEphemeral,
		MessageType:   MessageDanmu,
	}

	for _, tt := range []TargetType{TargetUser, TargetDevice, TargetChannel} {
		m := base
		m.TargetType = tt
		if err := m.Validate(); err == nil {
			t.Fatalf("%s without target_id must fail validation", tt)
		}
		m.TargetID = "target-1"
		if err := m.Validate(); err != nil {
			t.Fatalf("%s with target_id should validate: %v", tt, err)
		}
	}

	broadcast := base
	broadcast.TargetType = TargetBroadcast
	if err := broadcast.Validate(); err != nil {
		t.Fatalf("global broadcast may omit target_id: %v", err)
	}
}

func TestMessageEnvelopeValidateClassAndType(t *testing.T) {
	m := MessageEnvelope{
		TargetType:    TargetChannel,
		TargetID:      "live:100",
		DeliveryClass: "UNKNOWN",
		MessageType:   MessageDanmu,
	}
	if err := m.Validate(); err == nil {
		t.Fatal("invalid delivery class must fail")
	}

	m.DeliveryClass = DeliveryReliable
	m.MessageType = ""
	if err := m.Validate(); err == nil {
		t.Fatal("empty message type must fail")
	}

	m.MessageType = MessageDanmu
	if err := m.Validate(); err != nil {
		t.Fatalf("reliable is a valid class even though its pipeline is not implemented yet: %v", err)
	}
}

func TestDanmuChannelRoundTrip(t *testing.T) {
	for _, roomID := range []string{"r1", "live:100", "中文-room"} {
		channel := DanmuChannelID(roomID)
		got, ok := DanmuRoomID(channel)
		if !ok || got != roomID {
			t.Fatalf("round trip %q -> %q -> %q, ok=%v", roomID, channel, got, ok)
		}
	}
	if _, ok := DanmuRoomID("chat:general"); ok {
		t.Fatal("non-danmu channel must not be treated as a room")
	}
}
