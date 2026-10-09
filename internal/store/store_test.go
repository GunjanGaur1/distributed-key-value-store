package store

import (
	"errors"
	"testing"
)

func TestSetAndGet(t *testing.T) {
	s := New()
	s.Set("name", "Gunjan")

	got, err := s.Get("name")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got != "Gunjan" {
		t.Fatalf("expected Gunjan, got %q", got)
	}
}

func TestMissingKey(t *testing.T) {
	s := New()

	_, err := s.Get("missing")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestDelete(t *testing.T) {
	s := New()
	s.Set("language", "Go")
	s.Delete("language")

	_, err := s.Get("language")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected key to be missing, got %v", err)
	}
}

func TestLen(t *testing.T) {
	s := New()
	s.Set("a", "1")
	s.Set("b", "2")

	if got := s.Len(); got != 2 {
		t.Fatalf("expected 2 keys, got %d", got)
	}
}
