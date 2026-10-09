package raft

import (
	"context"
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

type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

type Entry struct {
	Index   int     `json:"index"`
	Term    uint64  `json:"term"`
	Command Command `json:"command"`
}

type VoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex int    `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}
type VoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}
type AppendRequest struct {
	Term         uint64  `json:"term"`
	LeaderID     string  `json:"leader_id"`
	PrevLogIndex int     `json:"prev_log_index"`
	PrevLogTerm  uint64  `json:"prev_log_term"`
	Entries      []Entry `json:"entries"`
	LeaderCommit int     `json:"leader_commit"`
}
type AppendResponse struct {
	Term       uint64 `json:"term"`
	Success    bool   `json:"success"`
	MatchIndex int    `json:"match_index"`
}
type ClientRequest struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}
type ClientResponse struct {
	OK         bool   `json:"ok"`
	Value      string `json:"value,omitempty"`
	Error      string `json:"error,omitempty"`
	LeaderID   string `json:"leader_id,omitempty"`
	LeaderAddr string `json:"leader_addr,omitempty"`
}
type HealthResponse struct {
	ID       string `json:"id"`
	State    string `json:"state"`
	Term     uint64 `json:"term"`
	LeaderID string `json:"leader_id"`
}

type Transport interface {
	RequestVote(ctx context.Context, addr string, req *VoteRequest) (*VoteResponse, error)
	AppendEntries(ctx context.Context, addr string, req *AppendRequest) (*AppendResponse, error)
}

type Config struct {
	ID          string
	Addr        string
	Peers       map[string]string
	DataDir     string
	ElectionMin time.Duration
	ElectionMax time.Duration
	Heartbeat   time.Duration
}
