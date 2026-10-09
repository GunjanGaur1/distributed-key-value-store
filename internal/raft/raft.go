package raft

import (
	"sync"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

type Node struct {
	mu              sync.Mutex
	ID              string
	State           State
	CurrentTerm     uint64
	VotedFor        string
	LeaderID        string
	LastHeartbeat   time.Time
	ElectionTimeout time.Duration
}

func NewNode(id string, electionTimeout time.Duration) *Node {
	return &Node{
		ID:              id,
		State:           Follower,
		ElectionTimeout: electionTimeout,
		LastHeartbeat:   time.Now(),
	}
}

func (n *Node) StartElection() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.CurrentTerm++
	n.State = Candidate
	n.VotedFor = n.ID
	n.LeaderID = ""
	n.LastHeartbeat = time.Now()
}

func (n *Node) BecomeLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.State == Candidate {
		n.State = Leader
		n.LeaderID = n.ID
	}
}

func (n *Node) ReceiveHeartbeat(term uint64, leaderID string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if term < n.CurrentTerm {
		return
	}

	if term > n.CurrentTerm {
		n.CurrentTerm = term
		n.VotedFor = ""
	}

	n.State = Follower
	n.LeaderID = leaderID
	n.LastHeartbeat = time.Now()
}

func (n *Node) GetState() (State, uint64, string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.State, n.CurrentTerm, n.LeaderID
}
