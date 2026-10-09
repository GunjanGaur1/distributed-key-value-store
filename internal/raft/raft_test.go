package raft

import (
	"context"
	"testing"
	"time"
)

type fakeTransport struct{}

func (fakeTransport) RequestVote(context.Context, string, *VoteRequest) (*VoteResponse, error) {
	return &VoteResponse{VoteGranted: true}, nil
}
func (fakeTransport) AppendEntries(context.Context, string, *AppendRequest) (*AppendResponse, error) {
	return &AppendResponse{Success: true}, nil
}

func TestNodeStartsFollower(t *testing.T) {
	n, err := NewNode(Config{ID: "n1", DataDir: t.TempDir()}, fakeTransport{})
	if err != nil {
		t.Fatal(err)
	}
	state := n.Health()
	if state.State != "follower" || state.Term != 0 {
		t.Fatalf("unexpected initial state: %+v", state)
	}
}
func TestVoteGrantedOnlyOncePerTerm(t *testing.T) {
	n, err := NewNode(Config{ID: "n2", DataDir: t.TempDir()}, fakeTransport{})
	if err != nil {
		t.Fatal(err)
	}
	a, err := n.RequestVote(context.Background(), &VoteRequest{Term: 1, CandidateID: "n1"})
	if err != nil || !a.VoteGranted {
		t.Fatalf("first vote: %+v %v", a, err)
	}
	b, err := n.RequestVote(context.Background(), &VoteRequest{Term: 1, CandidateID: "n3"})
	if err != nil || b.VoteGranted {
		t.Fatalf("second vote should be rejected: %+v %v", b, err)
	}
}
func TestNewerTermResetsVote(t *testing.T) {
	n, err := NewNode(Config{ID: "n2", DataDir: t.TempDir()}, fakeTransport{})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = n.RequestVote(context.Background(), &VoteRequest{Term: 1, CandidateID: "n1"})
	r, err := n.RequestVote(context.Background(), &VoteRequest{Term: 2, CandidateID: "n3"})
	if err != nil || !r.VoteGranted {
		t.Fatalf("expected vote in new term: %+v %v", r, err)
	}
	if n.Health().Term != 2 {
		t.Fatalf("expected term 2, got %d", n.Health().Term)
	}
}

var _ = time.Second
