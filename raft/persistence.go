package raft

import (
	"bytes"
	"encoding/gob"
	"log"
	"os"
	"sync"
)

// Package-level logger
var logger = log.New(os.Stderr, "[RAFT] ", log.LstdFlags)

// Persister maintains the persistent state of the Raft node
type Persister struct {
	mu        sync.Mutex
	raftstate []byte // Serialized Raft state
	snapshot  []byte // Latest snapshot data
}

// MakePersister creates a new Persister instance
func MakePersister() *Persister {
	return &Persister{
		raftstate: nil,
		snapshot:  nil,
	}
}

// Copy creates a deep copy of the Persister
func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	np := MakePersister()
	if ps.raftstate != nil {
		np.raftstate = make([]byte, len(ps.raftstate))
		copy(np.raftstate, ps.raftstate)
	}
	if ps.snapshot != nil {
		np.snapshot = make([]byte, len(ps.snapshot))
		copy(np.snapshot, ps.snapshot)
	}
	return np
}

// SaveRaftState persists the Raft state
func (ps *Persister) SaveRaftState(data []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = data
}

// ReadRaftState reads the persisted Raft state
func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.raftstate
}

// SaveStateAndSnapshot persists both Raft state and snapshot
func (ps *Persister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = state
	ps.snapshot = snapshot
}

// ReadSnapshot reads the persisted snapshot
func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.snapshot
}

// RaftStateSize returns the size of the persisted Raft state
func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// persist serializes and saves the Raft node's persistent state
func (rn *RaftNode) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	// Encode critical persistent state
	e.Encode(rn.currentTerm)
	e.Encode(rn.votedFor)
	e.Encode(rn.log)

	rn.persister.SaveRaftState(w.Bytes())
}

// readPersist restores the Raft node's persistent state
func (rn *RaftNode) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	// Decode in same order as encoding
	var currentTerm int
	var votedFor int
	var log []LogEntry

	if err := d.Decode(&currentTerm); err != nil {
		logger.Printf("Failed to decode currentTerm: %v", err)
		return
	}
	if err := d.Decode(&votedFor); err != nil {
		logger.Printf("Failed to decode votedFor: %v", err)
		return
	}
	if err := d.Decode(&log); err != nil {
		logger.Printf("Failed to decode log: %v", err)
		return
	}

	rn.currentTerm = currentTerm
	rn.votedFor = votedFor
	rn.log = log
}

// persistSnapshot saves both state and snapshot
func (rn *RaftNode) persistSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	e.Encode(rn.currentTerm)
	e.Encode(rn.votedFor)
	e.Encode(rn.log)

	rn.persister.SaveStateAndSnapshot(w.Bytes(), snapshot)
}
