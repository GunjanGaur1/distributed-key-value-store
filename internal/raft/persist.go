package raft

import (
	"encoding/json"
	"os"
	"path/filepath"
)

type diskState struct {
	CurrentTerm uint64            `json:"current_term"`
	VotedFor    string            `json:"voted_for"`
	Log         []Entry           `json:"log"`
	CommitIndex int               `json:"commit_index"`
	KV          map[string]string `json:"kv"`
}

func (n *Node) persistLocked() error {
	if n.dataDir == "" {
		return nil
	}
	if err := os.MkdirAll(n.dataDir, 0755); err != nil {
		return err
	}
	state := diskState{n.currentTerm, n.votedFor, n.log, n.commitIndex, n.kv}
	b, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(n.dataDir, "raft-state.tmp")
	dst := filepath.Join(n.dataDir, "raft-state.json")
	if err := os.WriteFile(tmp, b, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}

func (n *Node) load() error {
	if n.dataDir == "" {
		return nil
	}
	b, err := os.ReadFile(filepath.Join(n.dataDir, "raft-state.json"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var s diskState
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	n.currentTerm, n.votedFor, n.log, n.commitIndex, n.kv = s.CurrentTerm, s.VotedFor, s.Log, s.CommitIndex, s.KV
	if n.kv == nil {
		n.kv = make(map[string]string)
	}
	if n.lastApplied > n.commitIndex {
		n.lastApplied = n.commitIndex
	}
	return nil
}
