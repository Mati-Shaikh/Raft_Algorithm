package raft

import (
	"log"
	"time"
)

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rn *RaftNode) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// Reset election timeout if we get a valid request
	if args.Term >= rn.currentTerm {
		rn.lastHeard = time.Now()
	}

	reply.Term = rn.currentTerm
	reply.VoteGranted = false

	if args.Term < rn.currentTerm {
		return nil
	}

	if args.Term > rn.currentTerm {
		rn.stepDown(args.Term)
	}

	reply.Term = rn.currentTerm
	reply.VoteGranted = false

	if args.Term < rn.currentTerm {
		log.Printf("Node %d rejecting vote for stale term %d < %d",
			rn.id, args.Term, rn.currentTerm)
		return nil
	}

	if args.Term > rn.currentTerm {
		rn.stepDown(args.Term)
	}

	lastLogIndex, lastLogTerm := rn.lastLogInfo()
	upToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if (rn.votedFor == -1 || rn.votedFor == args.CandidateID) && upToDate {
		reply.VoteGranted = true
		rn.votedFor = args.CandidateID
		rn.lastHeard = time.Now()
		rn.persist()
		log.Printf("Node %d granted vote to %d for term %d",
			rn.id, args.CandidateID, args.Term)
	} else {
		log.Printf("Node %d denied vote to %d (votedFor=%d, upToDate=%v)",
			rn.id, args.CandidateID, rn.votedFor, upToDate)
	}

	return nil
}

func (rn *RaftNode) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply.Term = rn.currentTerm
	reply.Success = false

	// 1. Reply false if term < currentTerm
	if args.Term < rn.currentTerm {
		return nil
	}

	// Reset election timeout
	rn.lastHeard = time.Now()

	// 2. If RPC request contains higher term, update current term
	if args.Term > rn.currentTerm {
		rn.stepDown(args.Term)
	}

	// 3. Check if log contains entry at prevLogIndex matching prevLogTerm
	if args.PrevLogIndex >= 0 {
		if args.PrevLogIndex >= len(rn.log) {
			return nil
		}
		if rn.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			return nil
		}
	}

	// 4. Append any new entries
	if len(args.Entries) > 0 {
		// Find point of divergence
		insertIndex := args.PrevLogIndex + 1
		for i, entry := range args.Entries {
			if insertIndex+i >= len(rn.log) || rn.log[insertIndex+i].Term != entry.Term {
				rn.log = append(rn.log[:insertIndex+i], args.Entries[i:]...)
				break
			}
		}
		rn.persist()
	}

	// 5. Update commit index if necessary
	if args.LeaderCommit > rn.commitIndex {
		newCommitIndex := args.LeaderCommit
		if newCommitIndex > len(rn.log)-1 {
			newCommitIndex = len(rn.log) - 1
		}
		rn.commitIndex = newCommitIndex
		rn.applyLogs()
	}

	reply.Success = true
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
