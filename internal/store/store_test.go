package store

import (
	"errors"
	"testing"
)

func TestSetGetDelete(t *testing.T) {
	s := New()
	s.Set("name", "Gunjan")
	got, err := s.Get("name")
	if err != nil || got != "Gunjan" {
		t.Fatalf("got %q, err %v", got, err)
	}
	s.Delete("name")
	if _, err := s.Get("name"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected missing key, got %v", err)
	}
}

func TestLen(t *testing.T) {
	s := New()
	s.Set("a", "1")
	s.Set("b", "2")
	if s.Len() != 2 {
		t.Fatalf("expected 2 keys, got %d", s.Len())
	}
}
