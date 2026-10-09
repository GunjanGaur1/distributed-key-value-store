package raft

import (
	"testing"
	"time"
)

func TestNewNodeStartsAsFollower(t *testing.T) {
	n := NewNode("node-1", time.Second)
	state, term, leader := n.GetState()

	if state != Follower {
		t.Fatalf("expected follower, got %s", state)
	}
	if term != 0 {
		t.Fatalf("expected term 0, got %d", term)
	}
	if leader != "" {
		t.Fatalf("expected no leader, got %q", leader)
	}
}

func TestStartElection(t *testing.T) {
	n := NewNode("node-1", time.Second)
	n.StartElection()

	state, term, leader := n.GetState()
	if state != Candidate {
		t.Fatalf("expected candidate, got %s", state)
	}
	if term != 1 {
		t.Fatalf("expected term 1, got %d", term)
	}
	if leader != "" {
		t.Fatalf("expected no leader during election, got %q", leader)
	}

	n.mu.Lock()
	votedFor := n.VotedFor
	n.mu.Unlock()

	if votedFor != n.ID {
		t.Fatalf("expected self-vote for %s, got %s", n.ID, votedFor)
	}
}

func TestCandidateCanBecomeLeader(t *testing.T) {
	n := NewNode("node-1", time.Second)
	n.StartElection()
	n.BecomeLeader()

	state, _, leader := n.GetState()
	if state != Leader {
		t.Fatalf("expected leader, got %s", state)
	}
	if leader != "node-1" {
		t.Fatalf("expected node-1 as leader, got %q", leader)
	}
}

func TestHeartbeatMakesNodeFollower(t *testing.T) {
	n := NewNode("node-2", time.Second)
	n.StartElection()
	n.ReceiveHeartbeat(2, "node-1")

	state, term, leader := n.GetState()
	if state != Follower {
		t.Fatalf("expected follower, got %s", state)
	}
	if term != 2 {
		t.Fatalf("expected term 2, got %d", term)
	}
	if leader != "node-1" {
		t.Fatalf("expected node-1 as leader, got %q", leader)
	}
}

func TestRejectsOlderHeartbeat(t *testing.T) {
	n := NewNode("node-1", time.Second)
	n.StartElection()
	n.ReceiveHeartbeat(0, "old-leader")

	state, term, _ := n.GetState()
	if state != Candidate || term != 1 {
		t.Fatalf("older heartbeat changed state or term: state=%s term=%d", state, term)
	}
}
