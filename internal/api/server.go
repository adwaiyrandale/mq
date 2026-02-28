// Package api implements the gRPC service handlers
package api

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/adwaiy/mq/internal/broker"
	"github.com/adwaiy/mq/internal/consumer"
	"github.com/adwaiy/mq/internal/raft"
	"github.com/adwaiy/mq/internal/storage"
	mqpb "github.com/adwaiy/mq/proto"
)

// Server implements the MessageQueue gRPC service
type Server struct {
	mqpb.UnimplementedMessageQueueServer
	Storage      *storage.WAL
	Coordinator  *consumer.Coordinator
	OffsetCommit *consumer.OffsetCommit
	RaftManager  *raft.Manager
	Replicator   *broker.Replicator
	BrokerID     string
}

// Produce handles message production
func (s *Server) Produce(ctx context.Context, req *mqpb.ProduceRequest) (*mqpb.ProduceResponse, error) {
	partition := req.Partition
	if partition < 0 {
		partition = 0
	}

	// acks: 0 = fire and forget, 1 = leader only, -1 = all replicas
	acks := req.Acks
	if acks == 0 {
		// Fire and forget - return immediately
		go s.doProduce(req.Topic, partition, req)
		return &mqpb.ProduceResponse{
			Offset:    -1,
			Partition: partition,
			Timestamp: 0,
		}, nil
	}

	offset, timestamp, err := s.doProduce(req.Topic, partition, req)
	if err != nil {
		return nil, err
	}

	// If acks=-1 (all), wait for replication to ISR
	if acks == -1 {
		if !s.waitForReplication(req.Topic, partition, offset, 5*time.Second) {
			return nil, fmt.Errorf("replication timeout - not enough replicas in ISR")
		}
	}

	return &mqpb.ProduceResponse{
		Offset:    offset,
		Partition: partition,
		Timestamp: timestamp,
	}, nil
}

func (s *Server) doProduce(topic string, partition int32, req *mqpb.ProduceRequest) (int64, int64, error) {
	record := &storage.Record{
		Timestamp: time.Now().UnixMilli(),
		Key:       req.Key,
		Value:     req.Value,
		Headers:   req.Headers,
	}

	p, err := s.Storage.GetPartition(topic, partition)
	if err != nil {
		return 0, 0, fmt.Errorf("partition not found: %v", err)
	}

	offset, err := p.Append(record)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to append: %v", err)
	}

	return offset, record.Timestamp, nil
}

func (s *Server) waitForReplication(topic string, partition int32, offset int64, timeout time.Duration) bool {
	if pr, ok := s.Replicator.GetPartition(topic, partition); ok {
		return pr.IsISRQuorum()
	}
	return true
}

// Consume handles message consumption
func (s *Server) Consume(ctx context.Context, req *mqpb.ConsumeRequest) (*mqpb.ConsumeResponse, error) {
	p, err := s.Storage.GetPartition(req.Topic, req.Partition)
	if err != nil {
		return nil, fmt.Errorf("partition not found: %v", err)
	}

	var startOffset int64
	if req.Offset < 0 {
		startOffset = 0
	} else {
		startOffset = req.Offset
	}

	var messages []*mqpb.Message
	highWatermark := p.HighWatermark

	for offset := startOffset; offset < p.LogEndOffset; offset++ {
		record, err := p.Read(offset)
		if err != nil {
			log.Printf("Error reading offset %d: %v", offset, err)
			break
		}

		msg := &mqpb.Message{
			Key:       record.Key,
			Value:     record.Value,
			Headers:   record.Headers,
			Timestamp: record.Timestamp,
			Offset:    offset,
			Partition: req.Partition,
			Topic:     req.Topic,
		}
		messages = append(messages, msg)

		if len(messages) >= 100 {
			break
		}
	}

	return &mqpb.ConsumeResponse{
		Messages:      messages,
		HighWatermark: highWatermark,
	}, nil
}

// Subscribe handles streaming consumption
func (s *Server) Subscribe(req *mqpb.SubscribeRequest, stream mqpb.MessageQueue_SubscribeServer) error {
	p, err := s.Storage.GetPartition(req.Topic, req.Partition)
	if err != nil {
		return fmt.Errorf("partition not found: %v", err)
	}

	startOffset := req.StartOffset
	if startOffset < 0 {
		startOffset = 0
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-ticker.C:
			for startOffset < p.LogEndOffset {
				record, err := p.Read(startOffset)
				if err != nil {
					break
				}

				msg := &mqpb.Message{
					Key:       record.Key,
					Value:     record.Value,
					Headers:   record.Headers,
					Timestamp: record.Timestamp,
					Offset:    startOffset,
					Partition: req.Partition,
					Topic:     req.Topic,
				}

				if err := stream.Send(msg); err != nil {
					return err
				}

				startOffset++
			}
		}
	}
}

// CreateTopic creates a new topic
func (s *Server) CreateTopic(ctx context.Context, req *mqpb.CreateTopicRequest) (*mqpb.CreateTopicResponse, error) {
	err := s.Storage.CreateTopic(req.Topic, req.Partitions, storage.TopicConfig{
		RetentionMs:    7 * 24 * 60 * 60 * 1000,
		RetentionBytes: -1,
		SegmentBytes:   1024 * 1024 * 100,
	})
	if err != nil {
		return &mqpb.CreateTopicResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &mqpb.CreateTopicResponse{Success: true}, nil
}

// ListTopics lists all topics
func (s *Server) ListTopics(ctx context.Context, req *mqpb.ListTopicsRequest) (*mqpb.ListTopicsResponse, error) {
	topics := make([]string, 0)
	for name := range s.Storage.Topics() {
		topics = append(topics, name)
	}
	return &mqpb.ListTopicsResponse{Topics: topics}, nil
}

// DescribeTopic describes a topic
func (s *Server) DescribeTopic(ctx context.Context, req *mqpb.DescribeTopicRequest) (*mqpb.DescribeTopicResponse, error) {
	topic, ok := s.Storage.GetTopic(req.Topic)
	if !ok {
		return nil, fmt.Errorf("topic not found")
	}

	return &mqpb.DescribeTopicResponse{
		Topic:      req.Topic,
		Partitions: topic.PartitionCount,
	}, nil
}

// DeleteTopic deletes a topic
func (s *Server) DeleteTopic(ctx context.Context, req *mqpb.DeleteTopicRequest) (*mqpb.DeleteTopicResponse, error) {
	return &mqpb.DeleteTopicResponse{Success: false, Error: "not implemented"}, nil
}

// CommitOffset commits a consumer offset
func (s *Server) CommitOffset(ctx context.Context, req *mqpb.CommitOffsetRequest) (*mqpb.CommitOffsetResponse, error) {
	err := s.OffsetCommit.CommitOffset(req.GroupId, req.Topic, req.Partition, req.Offset)
	if err != nil {
		return &mqpb.CommitOffsetResponse{Success: false}, err
	}
	return &mqpb.CommitOffsetResponse{Success: true}, nil
}

// Seek seeks to an offset
func (s *Server) Seek(ctx context.Context, req *mqpb.SeekRequest) (*mqpb.SeekResponse, error) {
	return &mqpb.SeekResponse{Offset: req.Offset}, nil
}

// DescribeCluster describes the cluster
func (s *Server) DescribeCluster(ctx context.Context, req *mqpb.DescribeClusterRequest) (*mqpb.DescribeClusterResponse, error) {
	return &mqpb.DescribeClusterResponse{
		BrokerId:  1,
		ClusterId: "cluster-1",
		Brokers: []*mqpb.BrokerInfo{
			{Id: 1, Host: "localhost", Port: 9001},
		},
	}, nil
}

// JoinGroup handles consumer group joins
func (s *Server) JoinGroup(ctx context.Context, req *mqpb.JoinGroupRequest) (*mqpb.JoinGroupResponse, error) {
	memberID, generationID, err := s.Coordinator.JoinGroup(req.GroupId, req.MemberId, req.Topics)
	if err != nil {
		return nil, err
	}

	members := s.Coordinator.ListGroupMembers(req.GroupId)

	return &mqpb.JoinGroupResponse{
		MemberId:     memberID,
		GenerationId: generationID,
		Members:      members,
	}, nil
}

// SyncGroup handles consumer group sync
func (s *Server) SyncGroup(ctx context.Context, req *mqpb.SyncGroupRequest) (*mqpb.SyncGroupResponse, error) {
	assignment, err := s.Coordinator.PerformRebalance(req.GroupId)
	if err != nil {
		return nil, err
	}

	memberAssignment := assignment[req.MemberId]

	protoAssignment := make(map[string]*mqpb.PartitionAssignments)
	for topic, partitions := range memberAssignment {
		protoAssignment[topic] = &mqpb.PartitionAssignments{Partitions: partitions}
	}

	return &mqpb.SyncGroupResponse{
		Success:    true,
		Assignment: protoAssignment,
	}, nil
}

// Heartbeat handles consumer heartbeats
func (s *Server) Heartbeat(ctx context.Context, req *mqpb.HeartbeatRequest) (*mqpb.HeartbeatResponse, error) {
	err := s.Coordinator.Heartbeat(req.GroupId, req.MemberId)
	return &mqpb.HeartbeatResponse{Success: err == nil}, nil
}

// LeaveGroup handles consumer leaving
func (s *Server) LeaveGroup(ctx context.Context, req *mqpb.LeaveGroupRequest) (*mqpb.LeaveGroupResponse, error) {
	err := s.Coordinator.LeaveGroup(req.GroupId, req.MemberId)
	return &mqpb.LeaveGroupResponse{Success: err == nil}, nil
}

// RequestVote handles Raft vote requests
func (s *Server) RequestVote(ctx context.Context, req *mqpb.RequestVoteRequest) (*mqpb.RequestVoteResponse, error) {
	partitionID := "default"
	return s.RaftManager.HandleRequestVote(partitionID, req), nil
}

// AppendEntries handles Raft log replication
func (s *Server) AppendEntries(ctx context.Context, req *mqpb.AppendEntriesRequest) (*mqpb.AppendEntriesResponse, error) {
	partitionID := "default"
	return s.RaftManager.HandleAppendEntries(partitionID, req), nil
}

// Fetch handles follower replication fetching
func (s *Server) Fetch(ctx context.Context, req *mqpb.FetchRequest) (*mqpb.FetchResponse, error) {
	p, err := s.Storage.GetPartition(req.Topic, req.Partition)
	if err != nil {
		return &mqpb.FetchResponse{
			Topic:     req.Topic,
			Partition: req.Partition,
			ErrorCode: 1,
		}, nil
	}

	if req.ReplicaId != "" {
		s.Replicator.UpdateFetchOffset(req.Topic, req.Partition, req.ReplicaId, req.FetchOffset)
	}

	var messages []*mqpb.Message
	startOffset := req.FetchOffset
	maxBytes := req.MaxBytes
	if maxBytes == 0 {
		maxBytes = 1024 * 1024
	}

	bytesRead := 0
	for offset := startOffset; offset < p.LogEndOffset; offset++ {
		record, err := p.Read(offset)
		if err != nil {
			break
		}

		msg := &mqpb.Message{
			Key:       record.Key,
			Value:     record.Value,
			Headers:   record.Headers,
			Timestamp: record.Timestamp,
			Offset:    offset,
			Partition: req.Partition,
			Topic:     req.Topic,
		}
		messages = append(messages, msg)

		bytesRead += len(record.Key) + len(record.Value)
		if bytesRead >= int(maxBytes) {
			break
		}
	}

	return &mqpb.FetchResponse{
		Topic:         req.Topic,
		Partition:     req.Partition,
		HighWatermark: p.HighWatermark,
		LogEndOffset:  p.LogEndOffset,
		Messages:      messages,
		ErrorCode:     0,
	}, nil
}

// ProduceStream handles streaming production
func (s *Server) ProduceStream(stream mqpb.MessageQueue_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		resp, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}

		if err := stream.SendMsg(resp); err != nil {
			return err
		}
	}
}
