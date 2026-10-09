package raft

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

var ErrNotLeader = errors.New("not leader")

type Node struct {
	mu               sync.Mutex
	cfg              Config
	net              Transport
	state            State
	currentTerm      uint64
	votedFor         string
	leaderID         string
	log              []Entry // index 0 is a sentinel
	commitIndex      int
	lastApplied      int
	nextIndex        map[string]int
	matchIndex       map[string]int
	kv               map[string]string
	lastHeartbeat    time.Time
	electionDeadline time.Time
	dataDir          string
	stop             chan struct{}
	stopOnce         sync.Once
}

func NewNode(cfg Config, net Transport) (*Node, error) {
	if cfg.ElectionMin <= 0 {
		cfg.ElectionMin = 650 * time.Millisecond
	}
	if cfg.ElectionMax <= cfg.ElectionMin {
		cfg.ElectionMax = 1100 * time.Millisecond
	}
	if cfg.Heartbeat <= 0 {
		cfg.Heartbeat = 150 * time.Millisecond
	}
	n := &Node{cfg: cfg, net: net, state: Follower, log: []Entry{{Index: 0}}, nextIndex: map[string]int{}, matchIndex: map[string]int{}, kv: map[string]string{}, dataDir: cfg.DataDir, stop: make(chan struct{})}
	if err := n.load(); err != nil {
		return nil, fmt.Errorf("load raft state: %w", err)
	}
	n.resetElectionLocked()
	return n, nil
}

func (n *Node) Start() { go n.run() }
func (n *Node) Stop() {
	n.stopOnce.Do(func() { close(n.stop); n.mu.Lock(); _ = n.persistLocked(); n.mu.Unlock() })
}

func (n *Node) run() {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-n.stop:
			return
		case <-ticker.C:
			n.mu.Lock()
			state := n.state
			expired := time.Now().After(n.electionDeadline)
			n.mu.Unlock()
			if state == Leader {
				n.broadcastHeartbeat()
			} else if expired {
				n.startElection()
			}
		}
	}
}

func (n *Node) resetElectionLocked() {
	jitter := time.Duration(rand.Int63n(int64(n.cfg.ElectionMax - n.cfg.ElectionMin)))
	n.electionDeadline = time.Now().Add(n.cfg.ElectionMin + jitter)
	n.lastHeartbeat = time.Now()
}

func (n *Node) lastLogLocked() (int, uint64) {
	i := len(n.log) - 1
	return i, n.log[i].Term
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.currentTerm++
	term := n.currentTerm
	n.state = Candidate
	n.votedFor = n.cfg.ID
	n.leaderID = ""
	n.resetElectionLocked()
	lastIndex, lastTerm := n.lastLogLocked()
	_ = n.persistLocked()
	req := &VoteRequest{Term: term, CandidateID: n.cfg.ID, LastLogIndex: lastIndex, LastLogTerm: lastTerm}
	peers := make(map[string]string, len(n.cfg.Peers))
	for id, addr := range n.cfg.Peers {
		peers[id] = addr
	}
	n.mu.Unlock()

	votes := 1
	var votesMu sync.Mutex
	var wg sync.WaitGroup
	for id, addr := range peers {
		_ = id
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			resp, err := n.net.RequestVote(ctx, addr, req)
			if err != nil {
				return
			}
			n.mu.Lock()
			if resp.Term > n.currentTerm {
				n.currentTerm = resp.Term
				n.state = Follower
				n.votedFor = ""
				n.leaderID = ""
				n.resetElectionLocked()
				_ = n.persistLocked()
				n.mu.Unlock()
				return
			}
			stillCandidate := n.state == Candidate && n.currentTerm == term
			n.mu.Unlock()
			if resp.VoteGranted && stillCandidate {
				votesMu.Lock()
				votes++
				won := votes > (len(peers)+1)/2
				votesMu.Unlock()
				if won {
					n.becomeLeader(term)
				}
			}
		}(addr)
	}
	wg.Wait()
}

func (n *Node) becomeLeader(term uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.currentTerm != term || n.state != Candidate {
		return
	}
	n.state = Leader
	n.leaderID = n.cfg.ID
	last := len(n.log)
	for id := range n.cfg.Peers {
		n.nextIndex[id] = last
		n.matchIndex[id] = 0
	}
	n.resetElectionLocked()
	go n.broadcastHeartbeat()
}

func (n *Node) RequestVote(_ context.Context, req *VoteRequest) (*VoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	resp := &VoteResponse{Term: n.currentTerm}
	if req.Term < n.currentTerm {
		return resp, nil
	}
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.state = Follower
		n.votedFor = ""
		n.leaderID = ""
	}
	lastIndex, lastTerm := n.lastLogLocked()
	upToDate := req.LastLogTerm > lastTerm || (req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIndex)
	if (n.votedFor == "" || n.votedFor == req.CandidateID) && upToDate {
		n.votedFor = req.CandidateID
		n.resetElectionLocked()
		resp.VoteGranted = true
		_ = n.persistLocked()
	}
	resp.Term = n.currentTerm
	return resp, nil
}

func (n *Node) AppendEntries(_ context.Context, req *AppendRequest) (*AppendResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	resp := &AppendResponse{Term: n.currentTerm, Success: false, MatchIndex: 0}
	if req.Term < n.currentTerm {
		return resp, nil
	}
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""
	}
	n.state = Follower
	n.leaderID = req.LeaderID
	n.resetElectionLocked()
	if req.PrevLogIndex >= len(n.log) {
		resp.Term = n.currentTerm
		return resp, nil
	}
	if req.PrevLogIndex >= 0 && n.log[req.PrevLogIndex].Term != req.PrevLogTerm {
		if req.PrevLogIndex > 0 {
			n.log = n.log[:req.PrevLogIndex]
		}
		_ = n.persistLocked()
		resp.Term = n.currentTerm
		return resp, nil
	}
	for _, e := range req.Entries {
		if e.Index < len(n.log) {
			if n.log[e.Index].Term != e.Term {
				n.log = n.log[:e.Index]
				n.log = append(n.log, e)
			}
		} else if e.Index == len(n.log) {
			n.log = append(n.log, e)
		}
	}
	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = min(req.LeaderCommit, len(n.log)-1)
		n.applyCommittedLocked()
	}
	_ = n.persistLocked()
	resp.Term, resp.Success, resp.MatchIndex = n.currentTerm, true, len(n.log)-1
	return resp, nil
}

func (n *Node) broadcastHeartbeat() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	term, leaderCommit := n.currentTerm, n.commitIndex
	peers := make(map[string]string, len(n.cfg.Peers))
	for id, addr := range n.cfg.Peers {
		peers[id] = addr
	}
	n.mu.Unlock()
	for id, addr := range peers {
		go n.replicateTo(id, addr, term, leaderCommit)
	}
}

func (n *Node) replicateTo(id, addr string, term uint64, leaderCommit int) {
	n.mu.Lock()
	if n.state != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	next := n.nextIndex[id]
	if next < 1 {
		next = 1
	}
	if next > len(n.log) {
		next = len(n.log)
	}
	prev := next - 1
	req := &AppendRequest{Term: term, LeaderID: n.cfg.ID, PrevLogIndex: prev, PrevLogTerm: n.log[prev].Term, Entries: append([]Entry(nil), n.log[next:]...), LeaderCommit: leaderCommit}
	n.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
	defer cancel()
	resp, err := n.net.AppendEntries(ctx, addr, req)
	if err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if resp.Term > n.currentTerm {
		n.currentTerm = resp.Term
		n.state = Follower
		n.votedFor = ""
		n.leaderID = ""
		n.resetElectionLocked()
		_ = n.persistLocked()
		return
	}
	if n.state != Leader || n.currentTerm != term {
		return
	}
	if resp.Success {
		n.matchIndex[id] = resp.MatchIndex
		n.nextIndex[id] = resp.MatchIndex + 1
		n.advanceCommitLocked()
	} else if n.nextIndex[id] > 1 {
		n.nextIndex[id]--
	}
}

func (n *Node) advanceCommitLocked() {
	for idx := len(n.log) - 1; idx > n.commitIndex; idx-- {
		if n.log[idx].Term != n.currentTerm {
			continue
		}
		count := 1
		for id := range n.cfg.Peers {
			if n.matchIndex[id] >= idx {
				count++
			}
		}
		if count > (len(n.cfg.Peers)+1)/2 {
			n.commitIndex = idx
			n.applyCommittedLocked()
			_ = n.persistLocked()
			break
		}
	}
}

func (n *Node) applyCommittedLocked() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		e := n.log[n.lastApplied]
		switch e.Command.Op {
		case "put":
			n.kv[e.Command.Key] = e.Command.Value
		case "delete":
			delete(n.kv, e.Command.Key)
		}
	}
}

func (n *Node) Client(_ context.Context, req *ClientRequest) (*ClientResponse, error) {
	n.mu.Lock()
	if n.state != Leader {
		resp := &ClientResponse{OK: false, Error: ErrNotLeader.Error(), LeaderID: n.leaderID, LeaderAddr: n.cfg.Peers[n.leaderID]}
		if n.leaderID == n.cfg.ID {
			resp.LeaderAddr = n.cfg.Addr
		}
		n.mu.Unlock()
		return resp, nil
	}
	if req.Op == "get" {
		v, ok := n.kv[req.Key]
		n.mu.Unlock()
		if !ok {
			return &ClientResponse{OK: false, Error: "key not found"}, nil
		}
		return &ClientResponse{OK: true, Value: v}, nil
	}
	if req.Op != "put" && req.Op != "delete" {
		n.mu.Unlock()
		return &ClientResponse{OK: false, Error: "unsupported operation"}, nil
	}
	e := Entry{Index: len(n.log), Term: n.currentTerm, Command: Command{Op: req.Op, Key: req.Key, Value: req.Value}}
	n.log = append(n.log, e)
	term := n.currentTerm
	_ = n.persistLocked()
	n.mu.Unlock()

	n.broadcastHeartbeat()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		n.mu.Lock()
		committed := n.commitIndex >= e.Index
		leader := n.state == Leader && n.currentTerm == term
		n.mu.Unlock()
		if committed {
			return &ClientResponse{OK: true}, nil
		}
		if !leader {
			return &ClientResponse{OK: false, Error: ErrNotLeader.Error()}, nil
		}
		n.broadcastHeartbeat()
		time.Sleep(30 * time.Millisecond)
	}
	return &ClientResponse{OK: false, Error: "write timed out waiting for majority"}, nil
}

func (n *Node) Health() HealthResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	return HealthResponse{ID: n.cfg.ID, State: n.state.String(), Term: n.currentTerm, LeaderID: n.leaderID}
}
