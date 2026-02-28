package client

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	mqpb "github.com/adwaiy/mq/proto"
)

type Consumer struct {
	conn           *grpc.ClientConn
	client         mqpb.MessageQueueClient
	groupID        string
	memberID       string
	topics         []string
	assignment     map[string][]int32
	generationID   int32
	mu             sync.RWMutex
	stopCh         chan struct{}
	msgHandler     func(*mqpb.Message)
	autoCommit     bool
	commitInterval time.Duration
}

type ConsumerConfig struct {
	GroupID        string
	Topics         []string
	BrokerAddr     string
	AutoCommit     bool
	CommitInterval time.Duration
}

func NewConsumer(config ConsumerConfig) (*Consumer, error) {
	conn, err := grpc.Dial(config.BrokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %v", err)
	}

	commitInterval := config.CommitInterval
	if commitInterval == 0 {
		commitInterval = 5 * time.Second
	}

	return &Consumer{
		conn:           conn,
		client:         mqpb.NewMessageQueueClient(conn),
		groupID:        config.GroupID,
		memberID:       fmt.Sprintf("%s-%d", config.GroupID, time.Now().UnixNano()),
		topics:         config.Topics,
		assignment:     make(map[string][]int32),
		stopCh:         make(chan struct{}),
		autoCommit:     config.AutoCommit,
		commitInterval: commitInterval,
	}, nil
}

func (c *Consumer) Subscribe(handler func(*mqpb.Message)) error {
	c.msgHandler = handler

	// Join the consumer group
	ctx := context.Background()
	joinResp, err := c.client.JoinGroup(ctx, &mqpb.JoinGroupRequest{
		GroupId:  c.groupID,
		MemberId: c.memberID,
		Topics:   c.topics,
	})
	if err != nil {
		return fmt.Errorf("failed to join group: %v", err)
	}

	c.memberID = joinResp.MemberId
	c.generationID = joinResp.GenerationId
	log.Printf("Joined group %s as member %s (generation %d)", c.groupID, c.memberID, c.generationID)

	// Sync to get partition assignment
	syncResp, err := c.client.SyncGroup(ctx, &mqpb.SyncGroupRequest{
		GroupId:      c.groupID,
		MemberId:     c.memberID,
		GenerationId: c.generationID,
	})
	if err != nil {
		return fmt.Errorf("failed to sync group: %v", err)
	}

	if !syncResp.Success {
		return fmt.Errorf("sync group failed")
	}

	c.mu.Lock()
	for topic, partitions := range syncResp.Assignment {
		c.assignment[topic] = partitions.Partitions
	}
	c.mu.Unlock()

	log.Printf("Assigned partitions: %v", c.assignment)

	// Start heartbeat
	go c.heartbeatLoop()

	// Start consuming assigned partitions
	for topic, partitions := range c.assignment {
		for _, partition := range partitions {
			go c.consumePartition(topic, partition)
		}
	}

	return nil
}

func (c *Consumer) consumePartition(topic string, partition int32) {
	ctx := context.Background()

	// Get committed offset
	// For now, start from beginning (0) or could use Seek to get last committed
	startOffset := int64(0)

	stream, err := c.client.Subscribe(ctx, &mqpb.SubscribeRequest{
		GroupId:     c.groupID,
		Topic:       topic,
		Partition:   partition,
		StartOffset: startOffset,
	})
	if err != nil {
		log.Printf("Failed to subscribe to %s:%d: %v", topic, partition, err)
		return
	}

	commitTicker := time.NewTicker(c.commitInterval)
	defer commitTicker.Stop()

	var lastOffset int64 = -1

	go func() {
		for range commitTicker.C {
			if c.autoCommit && lastOffset >= 0 {
				c.CommitOffset(topic, partition, lastOffset)
			}
		}
	}()

	for {
		select {
		case <-c.stopCh:
			return
		default:
		}

		msg, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Printf("Stream error for %s:%d: %v", topic, partition, err)
			return
		}

		if c.msgHandler != nil {
			c.msgHandler(msg)
		}

		lastOffset = msg.Offset
	}
}

func (c *Consumer) heartbeatLoop() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			ctx := context.Background()
			_, err := c.client.Heartbeat(ctx, &mqpb.HeartbeatRequest{
				GroupId:      c.groupID,
				MemberId:     c.memberID,
				GenerationId: c.generationID,
			})
			if err != nil {
				log.Printf("Heartbeat failed: %v", err)
			}
		}
	}
}

func (c *Consumer) CommitOffset(topic string, partition int32, offset int64) error {
	ctx := context.Background()
	resp, err := c.client.CommitOffset(ctx, &mqpb.CommitOffsetRequest{
		GroupId:   c.groupID,
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
	})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("commit offset failed")
	}
	return nil
}

func (c *Consumer) Close() error {
	close(c.stopCh)

	ctx := context.Background()
	_, err := c.client.LeaveGroup(ctx, &mqpb.LeaveGroupRequest{
		GroupId:  c.groupID,
		MemberId: c.memberID,
	})
	if err != nil {
		log.Printf("LeaveGroup error: %v", err)
	}

	return c.conn.Close()
}

func (c *Consumer) Assignment() map[string][]int32 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make(map[string][]int32)
	for k, v := range c.assignment {
		result[k] = v
	}
	return result
}
