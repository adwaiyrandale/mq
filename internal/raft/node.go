package raft

import (
	"fmt"
	"math/rand"
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
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

type LogEntry struct {
	Term    int64
	Index   int64
	Command interface{}
}

type Node struct {
	mu sync.RWMutex

	// Node identity
	ID    string
	Peers []string // Other node IDs

	// Persistent state
	CurrentTerm int64
	VotedFor    string
	Log         []LogEntry

	// Volatile state
	CommitIndex int64
	LastApplied int64

	// Leader state
	NextIndex  map[string]int64
	MatchIndex map[string]int64

	// State machine
	state State

	// Channels
	stateCh       chan State
	voteCh        chan bool
	appendCh      chan AppendEntriesArgs
	electionTimer *time.Timer

	// Configuration
	electionTimeoutMin int64
	electionTimeoutMax int64
	heartbeatInterval  time.Duration

	// Callbacks
	SendRequestVote   func(peer string, args RequestVoteArgs) error
	SendAppendEntries func(peer string, args AppendEntriesArgs) error
	ApplyCommand      func(cmd interface{})
}

type RequestVoteArgs struct {
	Term         int64
	CandidateID  string
	LastLogIndex int64
	LastLogTerm  int64
}

type RequestVoteReply struct {
	Term        int64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int64
	LeaderID     string
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []LogEntry
	LeaderCommit int64
}

type AppendEntriesReply struct {
	Term    int64
	Success bool
}

func NewNode(id string, peers []string) *Node {
	n := &Node{
		ID:                 id,
		Peers:              peers,
		Log:                make([]LogEntry, 0),
		CurrentTerm:        0,
		VotedFor:           "",
		CommitIndex:        0,
		LastApplied:        0,
		state:              Follower,
		stateCh:            make(chan State, 10),
		voteCh:             make(chan bool, 10),
		electionTimeoutMin: 150,
		electionTimeoutMax: 300,
		heartbeatInterval:  50 * time.Millisecond,
		NextIndex:          make(map[string]int64),
		MatchIndex:         make(map[string]int64),
	}

	// Initialize leader state
	for _, peer := range peers {
		n.NextIndex[peer] = 1
		n.MatchIndex[peer] = 0
	}

	return n
}

func (n *Node) Run() {
	n.resetElectionTimer()

	for {
		n.mu.RLock()
		state := n.state
		n.mu.RUnlock()

		switch state {
		case Follower:
			n.runFollower()
		case Candidate:
			n.runCandidate()
		case Leader:
			n.runLeader()
		}
	}
}

func (n *Node) runFollower() {
	select {
	case <-n.electionTimer.C:
		n.becomeCandidate()
	}
}

func (n *Node) runCandidate() {
	n.mu.Lock()
	n.CurrentTerm++
	n.VotedFor = n.ID
	n.mu.Unlock()

	// Request votes from all peers
	for _, peer := range n.Peers {
		go func(peer string) {
			n.mu.RLock()
			args := RequestVoteArgs{
				Term:         n.CurrentTerm,
				CandidateID:  n.ID,
				LastLogIndex: n.getLastLogIndex(),
				LastLogTerm:  n.getLastLogTerm(),
			}
			n.mu.RUnlock()

			if n.SendRequestVote != nil {
				n.SendRequestVote(peer, args)
			}
		}(peer)
	}

	// Wait for votes or election timeout
	timeout := n.randomElectionTimeout()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			return // Election timeout, become candidate again

		default:
			n.mu.Lock()
			if n.state != Candidate {
				n.mu.Unlock()
				return // State changed
			}
			n.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (n *Node) runLeader() {
	// Send heartbeats immediately
	n.sendHeartbeats()

	ticker := time.NewTicker(n.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.mu.RLock()
			if n.state != Leader {
				n.mu.RUnlock()
				return
			}
			n.mu.RUnlock()
			n.sendHeartbeats()
		default:
			n.mu.RLock()
			if n.state != Leader {
				n.mu.RUnlock()
				return
			}
			n.mu.RUnlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (n *Node) becomeCandidate() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state = Candidate
	n.resetElectionTimer()
	fmt.Printf("Node %s became Candidate (term %d)\n", n.ID, n.CurrentTerm)
}

func (n *Node) BecomeLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Candidate {
		return
	}

	n.state = Leader
	fmt.Printf("Node %s became Leader (term %d)\n", n.ID, n.CurrentTerm)

	// Initialize leader state
	lastLogIndex := n.getLastLogIndex()
	for _, peer := range n.Peers {
		n.NextIndex[peer] = lastLogIndex + 1
		n.MatchIndex[peer] = 0
	}
}

func (n *Node) BecomeFollower(term int64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state = Follower
	n.CurrentTerm = term
	n.VotedFor = ""
	n.resetElectionTimer()
	fmt.Printf("Node %s became Follower (term %d)\n", n.ID, n.CurrentTerm)
}

func (n *Node) sendHeartbeats() {
	n.mu.RLock()
	defer n.mu.RUnlock()

	for _, peer := range n.Peers {
		go func(peer string) {
			n.mu.RLock()
			prevLogIndex := n.NextIndex[peer] - 1
			prevLogTerm := int64(0)
			if prevLogIndex > 0 && prevLogIndex <= int64(len(n.Log)) {
				prevLogTerm = n.Log[prevLogIndex-1].Term
			}

			args := AppendEntriesArgs{
				Term:         n.CurrentTerm,
				LeaderID:     n.ID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      nil, // Heartbeat
				LeaderCommit: n.CommitIndex,
			}
			n.mu.RUnlock()

			if n.SendAppendEntries != nil {
				n.SendAppendEntries(peer, args)
			}
		}(peer)
	}
}

func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := RequestVoteReply{Term: n.CurrentTerm}

	// Reply false if term < currentTerm
	if args.Term < n.CurrentTerm {
		reply.VoteGranted = false
		return reply
	}

	// If term > currentTerm, become follower
	if args.Term > n.CurrentTerm {
		n.CurrentTerm = args.Term
		n.VotedFor = ""
		n.state = Follower
	}

	// If votedFor is null or candidateId, and candidate's log is at least as up-to-date
	if (n.VotedFor == "" || n.VotedFor == args.CandidateID) &&
		n.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		n.VotedFor = args.CandidateID
		reply.VoteGranted = true
		n.resetElectionTimer()
		fmt.Printf("Node %s voted for %s (term %d)\n", n.ID, args.CandidateID, args.Term)
	} else {
		reply.VoteGranted = false
	}

	return reply
}

func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := AppendEntriesReply{Term: n.CurrentTerm}

	// Reply false if term < currentTerm
	if args.Term < n.CurrentTerm {
		reply.Success = false
		return reply
	}

	// Become follower if term >= currentTerm
	if args.Term > n.CurrentTerm || n.state == Candidate {
		n.CurrentTerm = args.Term
		n.VotedFor = ""
		n.state = Follower
	}

	n.resetElectionTimer()

	// Reply false if log doesn't contain an entry at prevLogIndex with prevLogTerm
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > int64(len(n.Log)) {
			reply.Success = false
			return reply
		}
		if n.Log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			reply.Success = false
			return reply
		}
	}

	// Append new entries
	if len(args.Entries) > 0 {
		// Delete conflicting entries and append new ones
		for i, entry := range args.Entries {
			index := args.PrevLogIndex + int64(i) + 1
			if index <= int64(len(n.Log)) {
				if n.Log[index-1].Term != entry.Term {
					n.Log = n.Log[:index-1]
					n.Log = append(n.Log, entry)
				}
			} else {
				n.Log = append(n.Log, entry)
			}
		}
	}

	// Update commit index
	if args.LeaderCommit > n.CommitIndex {
		n.CommitIndex = min(args.LeaderCommit, int64(len(n.Log)))
		n.applyCommitted()
	}

	reply.Success = true
	return reply
}

func (n *Node) isLogUpToDate(lastLogIndex, lastLogTerm int64) bool {
	myLastIndex := n.getLastLogIndex()
	myLastTerm := n.getLastLogTerm()

	if lastLogTerm != myLastTerm {
		return lastLogTerm > myLastTerm
	}
	return lastLogIndex >= myLastIndex
}

func (n *Node) getLastLogIndex() int64 {
	return int64(len(n.Log))
}

func (n *Node) getLastLogTerm() int64 {
	if len(n.Log) == 0 {
		return 0
	}
	return n.Log[len(n.Log)-1].Term
}

func (n *Node) resetElectionTimer() {
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	n.electionTimer = time.NewTimer(n.randomElectionTimeout())
}

func (n *Node) randomElectionTimeout() time.Duration {
	min := n.electionTimeoutMin
	max := n.electionTimeoutMax
	return time.Duration(min+rand.Int63n(max-min)) * time.Millisecond
}

func (n *Node) applyCommitted() {
	for n.LastApplied < n.CommitIndex {
		n.LastApplied++
		entry := n.Log[n.LastApplied-1]
		if n.ApplyCommand != nil {
			n.ApplyCommand(entry.Command)
		}
	}
}

func (n *Node) Propose(cmd interface{}) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return false
	}

	entry := LogEntry{
		Term:    n.CurrentTerm,
		Index:   n.getLastLogIndex() + 1,
		Command: cmd,
	}
	n.Log = append(n.Log, entry)

	return true
}

func (n *Node) State() State {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
}

func (n *Node) Term() int64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.CurrentTerm
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
