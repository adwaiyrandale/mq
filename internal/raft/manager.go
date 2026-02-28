package raft

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	mqpb "github.com/adwaiy/mq/proto"
)

type Manager struct {
	mu       sync.RWMutex
	nodes    map[string]*Node  // partition ID -> Raft node
	peers    map[string]string // peer ID -> "host:port"
	brokerID string
}

func NewManager(brokerID string) *Manager {
	return &Manager{
		nodes:    make(map[string]*Node),
		peers:    make(map[string]string),
		brokerID: brokerID,
	}
}

func (m *Manager) AddPeer(peerID, addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.peers[peerID] = addr
}

func (m *Manager) CreatePartitionRaft(partitionID string, peerIDs []string) *Node {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Filter out self from peers
	otherPeers := make([]string, 0)
	for _, peer := range peerIDs {
		if peer != m.brokerID {
			otherPeers = append(otherPeers, peer)
		}
	}

	node := NewNode(m.brokerID, otherPeers)

	// Set up send functions
	node.SendRequestVote = func(peer string, args RequestVoteArgs) error {
		return m.sendRequestVote(peer, args)
	}

	node.SendAppendEntries = func(peer string, args AppendEntriesArgs) error {
		return m.sendAppendEntries(peer, args)
	}

	m.nodes[partitionID] = node

	// Start the node
	go node.Run()

	return node
}

func (m *Manager) GetNode(partitionID string) (*Node, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	node, ok := m.nodes[partitionID]
	return node, ok
}

func (m *Manager) sendRequestVote(peerID string, args RequestVoteArgs) error {
	m.mu.RLock()
	addr, ok := m.peers[peerID]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("unknown peer: %s", peerID)
	}

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	client := mqpb.NewMessageQueueClient(conn)
	ctx := context.Background()

	_, err = client.RequestVote(ctx, &mqpb.RequestVoteRequest{
		Term:         args.Term,
		CandidateId:  args.CandidateID,
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
	})

	return err
}

func (m *Manager) sendAppendEntries(peerID string, args AppendEntriesArgs) error {
	m.mu.RLock()
	addr, ok := m.peers[peerID]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("unknown peer: %s", peerID)
	}

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	client := mqpb.NewMessageQueueClient(conn)
	ctx := context.Background()

	entries := make([]*mqpb.LogEntry, len(args.Entries))
	for i, e := range args.Entries {
		entries[i] = &mqpb.LogEntry{
			Term:  e.Term,
			Index: e.Index,
		}
	}

	_, err = client.AppendEntries(ctx, &mqpb.AppendEntriesRequest{
		Term:         args.Term,
		LeaderId:     args.LeaderID,
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: args.LeaderCommit,
	})

	return err
}

func (m *Manager) HandleRequestVote(partitionID string, req *mqpb.RequestVoteRequest) *mqpb.RequestVoteResponse {
	node, ok := m.GetNode(partitionID)
	if !ok {
		return &mqpb.RequestVoteResponse{VoteGranted: false}
	}

	args := RequestVoteArgs{
		Term:         req.Term,
		CandidateID:  req.CandidateId,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	}

	reply := node.HandleRequestVote(args)

	// Check if we became follower
	if reply.VoteGranted && node.State() == Candidate && req.Term > node.Term() {
		node.BecomeFollower(req.Term)
	}

	return &mqpb.RequestVoteResponse{
		Term:        reply.Term,
		VoteGranted: reply.VoteGranted,
	}
}

func (m *Manager) HandleAppendEntries(partitionID string, req *mqpb.AppendEntriesRequest) *mqpb.AppendEntriesResponse {
	node, ok := m.GetNode(partitionID)
	if !ok {
		return &mqpb.AppendEntriesResponse{Success: false}
	}

	entries := make([]LogEntry, len(req.Entries))
	for i, e := range req.Entries {
		entries[i] = LogEntry{
			Term:  e.Term,
			Index: e.Index,
		}
	}

	args := AppendEntriesArgs{
		Term:         req.Term,
		LeaderID:     req.LeaderId,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: req.LeaderCommit,
	}

	reply := node.HandleAppendEntries(args)

	// If leader is valid, stay/convert to follower
	if reply.Success && req.Term >= node.Term() {
		if node.State() == Candidate || node.State() == Leader {
			node.BecomeFollower(req.Term)
		}
	}

	return &mqpb.AppendEntriesResponse{
		Term:    reply.Term,
		Success: reply.Success,
	}
}
