package main

import (
	"flag"
	"log"
	"net"
	"strings"

	"google.golang.org/grpc"

	"github.com/adwaiy/mq/internal/api"
	"github.com/adwaiy/mq/internal/broker"
	"github.com/adwaiy/mq/internal/consumer"
	"github.com/adwaiy/mq/internal/raft"
	"github.com/adwaiy/mq/internal/storage"
	mqpb "github.com/adwaiy/mq/proto"
)

// Broker holds all the dependencies for the message queue broker
type Broker struct {
	storage      *storage.WAL
	coordinator  *consumer.Coordinator
	offsetCommit *consumer.OffsetCommit
	raftManager  *raft.Manager
	replicator   *broker.Replicator
	brokerID     string
	port         string
}

// NewBroker creates a new broker instance
func NewBroker(dataDir string, brokerID string, port string, peers map[string]string) (*Broker, error) {
	wal, err := storage.NewWAL(storage.Config{
		DataDir:     dataDir,
		SegmentSize: 1024 * 1024 * 100,
	})
	if err != nil {
		return nil, err
	}

	rm := raft.NewManager(brokerID)
	for peerID, addr := range peers {
		rm.AddPeer(peerID, addr)
	}

	b := &Broker{
		storage:      wal,
		offsetCommit: consumer.NewOffsetCommit(wal),
		raftManager:  rm,
		replicator:   broker.NewReplicator(),
		brokerID:     brokerID,
		port:         port,
	}

	b.coordinator = consumer.NewCoordinator(func(topic string) (int32, bool) {
		t, ok := wal.GetTopic(topic)
		if !ok {
			return 0, false
		}
		return t.PartitionCount, true
	})

	if err := b.createInternalTopics(); err != nil {
		return nil, err
	}

	return b, nil
}

func (b *Broker) createInternalTopics() error {
	topics := []string{"__consumer_offsets"}
	for _, topic := range topics {
		if _, exists := b.storage.GetTopic(topic); !exists {
			if err := b.storage.CreateTopic(topic, 1, storage.TopicConfig{}); err != nil {
				return err
			}
			log.Printf("Created internal topic: %s", topic)
		}
	}
	return nil
}

func main() {
	id := flag.String("id", "broker-1", "Broker ID")
	port := flag.String("port", "9001", "Broker port")
	dataDir := flag.String("data", "./data", "Data directory")
	peersStr := flag.String("peers", "", "Comma-separated list of peers (id:host:port)")
	flag.Parse()

	// Parse peers
	peers := parsePeers(*peersStr)

	brokerID := *id
	portStr := ":" + *port

	b, err := NewBroker(*dataDir, brokerID, portStr, peers)
	if err != nil {
		log.Fatalf("Failed to create broker: %v", err)
	}

	// Create gRPC server with API handlers
	server := &api.Server{
		Storage:      b.storage,
		Coordinator:  b.coordinator,
		OffsetCommit: b.offsetCommit,
		RaftManager:  b.raftManager,
		Replicator:   b.replicator,
		BrokerID:     b.brokerID,
	}

	lis, err := net.Listen("tcp", portStr)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	mqpb.RegisterMessageQueueServer(grpcServer, server)

	log.Printf("Broker %s listening on %s", brokerID, portStr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func parsePeers(peersStr string) map[string]string {
	peers := make(map[string]string)
	if peersStr == "" {
		return peers
	}

	for _, peer := range strings.Split(peersStr, ",") {
		parts := strings.Split(peer, ":")
		if len(parts) >= 2 {
			peerID := parts[0]
			addr := parts[1]
			if len(parts) >= 3 {
				addr = addr + ":" + parts[2]
			}
			peers[peerID] = addr
		}
	}
	return peers
}
