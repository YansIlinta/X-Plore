package core

import "testing"

func TestDeliverEnvelopeUserTarget(t *testing.T) {
	h := mkHub(t)
	c1 := addConn(t, h, "u1", "web", "r1")
	c2 := addConn(t, h, "u1", "mobile", "r2")
	_ = addConn(t, h, "u2", "web", "r1")

	env := MessageEnvelope{
		TargetType:    TargetUser,
		TargetID:      "u1",
		DeliveryClass: DeliveryEphemeral,
		MessageType:   MessageDanmu,
	}
	payload := []byte(`{"type":"test"}`)
	res, err := h.DeliverEnvelope(env, payload)
	if err != nil {
		t.Fatalf("DeliverEnvelope: %v", err)
	}
	if res.Delivered != 2 {
		t.Fatalf("delivered=%d want 2", res.Delivered)
	}
	for _, c := range []*Client{c1, c2} {
		select {
		case got := <-c.sendCh:
			if string(got) != string(payload) {
				t.Fatalf("payload=%q want %q", got, payload)
			}
		default:
			t.Fatal("u1 connection should receive user-target payload")
		}
	}
}

func TestDeliverEnvelopeDeviceTargetIsUserScoped(t *testing.T) {
	h := mkHub(t)
	u1web := addConn(t, h, "u1", "web", "r1")
	u2web := addConn(t, h, "u2", "web", "r1")

	env := MessageEnvelope{
		TargetType:    TargetDevice,
		TargetUserID:  "u1",
		TargetID:      "web",
		DeliveryClass: DeliveryEphemeral,
		MessageType:   MessageDanmu,
	}
	res, err := h.DeliverEnvelope(env, []byte(`{"type":"device"}`))
	if err != nil {
		t.Fatalf("DeliverEnvelope: %v", err)
	}
	if res.Delivered != 1 {
		t.Fatalf("delivered=%d want 1", res.Delivered)
	}
	select {
	case <-u1web.sendCh:
	default:
		t.Fatal("u1/web should receive device-target payload")
	}
	select {
	case <-u2web.sendCh:
		t.Fatal("u2/web must not receive u1/web device-target payload")
	default:
	}
}

func TestDeliverEnvelopeChannelTarget(t *testing.T) {
	h := mkHub(t)
	c1 := addConn(t, h, "u1", "web", "r1")
	_ = addConn(t, h, "u2", "web", "r2")

	env := MessageEnvelope{
		TargetType:    TargetChannel,
		TargetID:      DanmuChannelID("r1"),
		DeliveryClass: DeliveryEphemeral,
		MessageType:   MessageDanmu,
	}
	res, err := h.DeliverEnvelope(env, []byte(`{"type":"channel"}`))
	if err != nil {
		t.Fatalf("DeliverEnvelope: %v", err)
	}
	if res.Delivered != 1 {
		t.Fatalf("delivered=%d want 1", res.Delivered)
	}
	select {
	case <-c1.sendCh:
	default:
		t.Fatal("r1 subscriber should receive channel-target payload")
	}
}

func TestDeliverEnvelopeBroadcast(t *testing.T) {
	h := mkHub(t)
	c1 := addConn(t, h, "u1", "web", "r1")
	c2 := addConn(t, h, "u2", "mobile", "r2")

	env := MessageEnvelope{
		TargetType:    TargetBroadcast,
		DeliveryClass: DeliveryEphemeral,
		MessageType:   MessageDanmu,
	}
	res, err := h.DeliverEnvelope(env, []byte(`{"type":"broadcast"}`))
	if err != nil {
		t.Fatalf("DeliverEnvelope: %v", err)
	}
	if res.Delivered != 2 {
		t.Fatalf("delivered=%d want 2", res.Delivered)
	}
	for _, c := range []*Client{c1, c2} {
		select {
		case <-c.sendCh:
		default:
			t.Fatal("every local connection should receive broadcast payload")
		}
	}
}

func TestDeliverEnvelopeRejectsInvalidDeviceAndEmptyPayload(t *testing.T) {
	h := mkHub(t)
	env := MessageEnvelope{
		TargetType:    TargetDevice,
		TargetID:      "web",
		DeliveryClass: DeliveryEphemeral,
		MessageType:   MessageDanmu,
	}
	if _, err := h.DeliverEnvelope(env, []byte(`{}`)); err == nil {
		t.Fatal("DEVICE target without target_user_id must fail")
	}

	env = MessageEnvelope{
		TargetType:    TargetBroadcast,
		DeliveryClass: DeliveryEphemeral,
		MessageType:   MessageDanmu,
	}
	if _, err := h.DeliverEnvelope(env, nil); err == nil {
		t.Fatal("empty WebSocket delivery payload must fail")
	}
}
