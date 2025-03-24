package raft

import (
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command interface{}
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type RaftNode struct {
	mu        sync.Mutex
	id        int
	peers     []*RaftNode
	persister *Persister

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int
	state       State

	nextIndex  []int
	matchIndex []int

	electionTimeout time.Duration
	lastHeard       time.Time

	applyCh  chan ApplyMsg
	clientCh chan ClientRequest

	currentLeader int // Track the current leader
	electionTimer *time.Timer

	stopCh chan struct{}
}

type ClientRequest struct {
	Op    string
	Key   string
	Value string
	Resp  chan ClientResponse
}

type ClientResponse struct {
	Success bool
	Value   string
	Leader  int
}

func NewRaftNode(id int, peers []*RaftNode, persister *Persister, applyCh chan ApplyMsg) *RaftNode {
	rn := &RaftNode{
		id:              id,
		peers:           peers,
		persister:       persister,
		applyCh:         applyCh,
		clientCh:        make(chan ClientRequest),
		stopCh:          make(chan struct{}),
		electionTimeout: time.Duration(150+rand.Intn(150)) * time.Millisecond,
		state:           Follower,
	}

	// Initialize from persisted state
	rn.readPersist(persister.ReadRaftState())

	return rn
}

func (rn *RaftNode) GetLeaderID() int {
	log.Printf("Node %d attempting to get leader ID", rn.id)
	rn.mu.Lock()
	defer rn.mu.Unlock()
	log.Printf("Node %d got leader ID: %d", rn.id, rn.currentLeader)
	return rn.currentLeader
}

// Modified startElection (with lock safety)
func (rn *RaftNode) startElection() {
	rn.mu.Lock()

	// Only start election if we're a follower
	if rn.state != Follower {
		rn.mu.Unlock()
		return
	}

	rn.state = Candidate
	rn.currentTerm++
	rn.votedFor = rn.id
	currentTerm := rn.currentTerm
	rn.persist()

	// Release lock before RPC calls
	rn.mu.Unlock()

	log.Printf("Node %d starting election for term %d", rn.id, currentTerm)

	// Get votes without holding the lock
	votes := 1 // self-vote
	var wg sync.WaitGroup

	for _, peer := range rn.peers {
		if peer == nil || peer.id == rn.id {
			continue
		}

		wg.Add(1)
		go func(p *RaftNode) {
			defer wg.Done()

			args := RequestVoteArgs{
				Term:         currentTerm,
				CandidateID:  rn.id,
				LastLogIndex: len(rn.log) - 1,
				LastLogTerm:  0,
			}
			if len(rn.log) > 0 {
				args.LastLogTerm = rn.log[len(rn.log)-1].Term
			}

			var reply RequestVoteReply
			if p.RequestVote(&args, &reply) == nil && reply.VoteGranted {
				rn.mu.Lock()
				if rn.state == Candidate && rn.currentTerm == currentTerm {
					votes++
					if votes > len(rn.peers)/2 {
						rn.becomeLeader()
					}
				}
				rn.mu.Unlock()
			}
		}(peer)
	}

	// Wait for election results with timeout
	go func() {
		wg.Wait()
	}()
}

// Modified becomeLeader
func (rn *RaftNode) becomeLeader() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.state != Candidate {
		return
	}

	rn.state = Leader
	rn.currentLeader = rn.id
	log.Printf("Node %d became leader for term %d", rn.id, rn.currentTerm)

	// Initialize leader state
	rn.nextIndex = make([]int, len(rn.peers))
	rn.matchIndex = make([]int, len(rn.peers))
	for i := range rn.nextIndex {
		rn.nextIndex[i] = len(rn.log) + 1
	}

	// Send initial heartbeat
	go rn.broadcastAppendEntries(true)
}

// Modified Run method
func (rn *RaftNode) Run() {
	rn.electionTimer = time.NewTimer(rn.electionTimeout)

	for {
		select {
		case <-rn.stopCh:
			rn.electionTimer.Stop()
			return

		case <-rn.electionTimer.C:
			rn.mu.Lock()
			if rn.state != Leader {
				rn.startElection()
			}
			rn.resetElectionTimer()
			rn.mu.Unlock()

		case req := <-rn.clientCh:
			rn.handleClientRequest(req)
		}
	}
}

func (rn *RaftNode) resetElectionTimer() {
	rn.electionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond
	rn.electionTimer.Reset(rn.electionTimeout)
}

func (rn *RaftNode) runFollower() {
	timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond

	select {
	case <-time.After(timeout):
		rn.mu.Lock()
		if time.Since(rn.lastHeard) >= timeout && rn.state == Follower {
			rn.startElection()
		}
		rn.mu.Unlock()
	case <-rn.stopCh:
		return
	}
}

func (rn *RaftNode) runCandidate() {
	rn.mu.Lock()
	if rn.state != Candidate {
		rn.mu.Unlock()
		return
	}

	currentTerm := rn.currentTerm
	lastLogIndex, lastLogTerm := rn.lastLogInfo()
	rn.mu.Unlock()

	log.Printf("Node %d starting election for term %d", rn.id, currentTerm)

	var votes int32 = 1 // self-vote
	var wg sync.WaitGroup
	voteCh := make(chan bool, len(rn.peers))

	// Request votes from all peers
	for _, peer := range rn.peers {
		if peer == nil || peer.id == rn.id {
			continue
		}

		wg.Add(1)
		go func(p *RaftNode) {
			defer wg.Done()

			args := RequestVoteArgs{
				Term:         currentTerm,
				CandidateID:  rn.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			var reply RequestVoteReply
			if err := p.RequestVote(&args, &reply); err == nil && reply.VoteGranted {
				voteCh <- true
			}
		}(peer)
	}

	// Wait for votes or timeout
	electionTimeout := time.After(rn.electionTimeout)

	go func() {
		wg.Wait()
		close(voteCh)
	}()

	for {
		select {
		case v, ok := <-voteCh:
			if !ok {
				// All votes received
				return
			}
			if v {
				atomic.AddInt32(&votes, 1)
				if int(votes) > len(rn.peers)/2 {
					rn.becomeLeader()
					return
				}
			}
		case <-electionTimeout:
			log.Printf("Node %d election timeout for term %d", rn.id, currentTerm)
			return
		case <-rn.stopCh:
			return
		}
	}
}
func (rn *RaftNode) requestVotes(term int, voteCh chan bool) {
	rn.mu.Lock()
	lastLogIndex, lastLogTerm := rn.lastLogInfo()
	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  rn.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rn.mu.Unlock()

	for _, peer := range rn.peers {
		go func(p *RaftNode) {
			var reply RequestVoteReply
			err := p.RequestVote(&args, &reply)
			if err == nil {
				voteCh <- reply.VoteGranted
			} else {
				voteCh <- false
			}
		}(peer)
	}
}

func (rn *RaftNode) runLeader() {
	rn.mu.Lock()
	rn.nextIndex = make([]int, len(rn.peers))
	rn.matchIndex = make([]int, len(rn.peers))
	for i := range rn.nextIndex {
		rn.nextIndex[i] = len(rn.log)
	}
	rn.mu.Unlock()

	heartbeatTicker := time.NewTicker(50 * time.Millisecond)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-heartbeatTicker.C:
			rn.broadcastAppendEntries(true)
		case req := <-rn.clientCh:
			rn.handleClientRequest(req)
		case <-rn.stopCh:
			return
		}
	}
}
func (rn *RaftNode) broadcastAppendEntries(isHeartbeat bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	for i, peer := range rn.peers {
		if peer == nil || peer.id == rn.id {
			continue
		}

		nextIdx := rn.nextIndex[i]
		prevLogIndex := nextIdx - 1
		var prevLogTerm int
		var entries []LogEntry

		// Safety checks for log access
		if prevLogIndex >= 0 && prevLogIndex < len(rn.log) {
			prevLogTerm = rn.log[prevLogIndex].Term
		} else {
			prevLogTerm = -1
		}

		if !isHeartbeat && nextIdx < len(rn.log) {
			entries = rn.log[nextIdx:]
		}

		args := AppendEntriesArgs{
			Term:         rn.currentTerm,
			LeaderID:     rn.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rn.commitIndex,
		}

		go func(peer *RaftNode, args AppendEntriesArgs, peerIdx int) {
			var reply AppendEntriesReply
			err := peer.AppendEntries(&args, &reply) // Now properly handling the error
			if err != nil {
				log.Printf("AppendEntries RPC failed to node %d: %v", peer.id, err)
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			if reply.Term > rn.currentTerm {
				rn.stepDown(reply.Term)
				return
			}

			if reply.Success {
				rn.nextIndex[peerIdx] = args.PrevLogIndex + len(args.Entries) + 1
				rn.matchIndex[peerIdx] = rn.nextIndex[peerIdx] - 1
				rn.updateCommitIndex()
			} else {
				rn.nextIndex[peerIdx]--
			}
		}(peer, args, i)
	}
}

func (rn *RaftNode) updateCommitIndex() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if len(rn.log) == 0 {
		return
	}

	// Make a copy including leader's log length
	matchIndexes := make([]int, len(rn.peers)+1)
	for i := range rn.peers {
		matchIndexes[i] = rn.matchIndex[i]
	}
	matchIndexes[len(rn.peers)] = len(rn.log) - 1 // Leader's log

	sort.Ints(matchIndexes)
	N := matchIndexes[len(matchIndexes)/2]

	// Only commit entries from current term
	if N > rn.commitIndex && N < len(rn.log) && rn.log[N].Term == rn.currentTerm {
		rn.commitIndex = N
		rn.applyLogs()
	}
}

func (rn *RaftNode) applyLogs() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	for rn.lastApplied < rn.commitIndex && rn.lastApplied+1 < len(rn.log) {
		rn.lastApplied++
		entry := rn.log[rn.lastApplied]
		select {
		case rn.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: rn.lastApplied,
		}:
		default:
			// Channel full - back off and retry
			time.Sleep(10 * time.Millisecond)
			return
		}
	}
}

func (rn *RaftNode) stepDown(term int) {
	rn.state = Follower
	rn.currentTerm = term
	rn.votedFor = -1
	rn.lastHeard = time.Now()
	rn.persist()
}

func (rn *RaftNode) lastLogInfo() (index, term int) {
	if len(rn.log) == 0 {
		return -1, -1
	}
	last := rn.log[len(rn.log)-1]
	return len(rn.log) - 1, last.Term
}

// In raft/raft.go
func (rn *RaftNode) handleClientRequest(req ClientRequest) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.state != Leader {
		select {
		case req.Resp <- ClientResponse{Leader: rn.getLeaderID()}:
		default: // Don't block if client isn't listening
		}
		return
	}

	entry := LogEntry{
		Term:    rn.currentTerm,
		Command: req,
	}
	rn.log = append(rn.log, entry)
	rn.persist()

	// Start replication in background
	go rn.broadcastAppendEntries(false)

	// Send preliminary response
	select {
	case req.Resp <- ClientResponse{Success: true}:
	default: // Don't block if client isn't listening
	}
}

func (rn *RaftNode) getLeaderID() int {
	// Simple implementation - in real system would track leader
	return -1
}

// Add these methods to your RaftNode struct in raft.go

// GetApplyChannel returns a receive-only channel for apply messages
func (rn *RaftNode) GetApplyChannel() <-chan ApplyMsg {
	return rn.applyCh
}

// GetClientChannel returns a send-only channel for client requests
func (rn *RaftNode) GetClientChannel() chan<- ClientRequest {
	return rn.clientCh
}

func getPeerIDs(peers []*RaftNode) []int {
	ids := make([]int, 0, len(peers))
	for _, p := range peers {
		if p != nil {
			ids = append(ids, p.id)
		}
	}
	return ids
}

func (rn *RaftNode) UpdatePeers(peers []*RaftNode) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.peers = peers
	log.Printf("Node %d updated peers: %v", rn.id, getPeerIDs(peers))
}

// SetClientChannel sets the client channel after initialization
func (rn *RaftNode) SetClientChannel(ch chan ClientRequest) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.clientCh = ch
}

// Add these methods to your RaftNode struct

// GetID returns the node's ID
func (rn *RaftNode) GetID() int {
	return rn.id
}

// NodeState represents a snapshot of a node's state
type NodeState struct {
	State    State
	Term     int
	LeaderID int
}

// GetState returns the current node state
func (rn *RaftNode) GetState() NodeState {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	return NodeState{
		State:    rn.state,
		Term:     rn.currentTerm,
		LeaderID: rn.getLeaderID(),
	}
}
