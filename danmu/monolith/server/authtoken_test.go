package main

import (
	"testing"
	"time"
)

func TestTokenIssuerRoundTrip(t *testing.T) {
	ti := NewTokenIssuer("shared-secret")
	token, expiresAt := ti.Issue("u1", "room-1", time.Minute)

	got, err := ti.Verify(token, "u1", "room-1")
	if err != nil {
		t.Fatalf("Verify failed: %v", err)
	}
	if got.Unix() != expiresAt.Unix() {
		t.Errorf("expiresAt mismatch: got %v want %v", got, expiresAt)
	}
}

func TestTokenIssuerRejectsWrongBinding(t *testing.T) {
	ti := NewTokenIssuer("shared-secret")
	token, _ := ti.Issue("u1", "room-1", time.Minute)

	if _, err := ti.Verify(token, "u2", "room-1"); err != errInvalidToken {
		t.Errorf("expected errInvalidToken for wrong uid, got %v", err)
	}
	if _, err := ti.Verify(token, "u1", "room-2"); err != errInvalidToken {
		t.Errorf("expected errInvalidToken for wrong room, got %v", err)
	}
}

func TestTokenIssuerRejectsTampering(t *testing.T) {
	ti := NewTokenIssuer("shared-secret")
	token, _ := ti.Issue("u1", "room-1", time.Minute)

	if _, err := ti.Verify(token+"x", "u1", "room-1"); err == nil {
		t.Error("expected error for tampered token")
	}

	other := NewTokenIssuer("different-secret")
	if _, err := other.Verify(token, "u1", "room-1"); err != errInvalidToken {
		t.Errorf("expected errInvalidToken for wrong signing key, got %v", err)
	}
}

func TestTokenIssuerExpiry(t *testing.T) {
	ti := NewTokenIssuer("shared-secret")
	token, _ := ti.Issue("u1", "room-1", -time.Second) // 已过期

	if _, err := ti.Verify(token, "u1", "room-1"); err != errTokenExpired {
		t.Errorf("expected errTokenExpired, got %v", err)
	}
}
