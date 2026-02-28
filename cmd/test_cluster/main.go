package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	mqpb "github.com/adwaiy/mq/proto"
)

func main() {
	fmt.Println("=== Multi-Broker Cluster Test ===\n")

	// Build broker
	fmt.Println("Building broker...")
	build := exec.Command("go", "build", "-o", "bin/broker", "./cmd/broker")
	build.Dir = "/Users/adwaiy/dev/mq"
	if err := build.Run(); err != nil {
		log.Fatalf("Failed to build: %v", err)
	}

	// Clean up old data
	os.RemoveAll("/Users/adwaiy/dev/mq/data/test-cluster")
	os.MkdirAll("/Users/adwaiy/dev/mq/data/test-cluster/node1", 0755)
	os.MkdirAll("/Users/adwaiy/dev/mq/data/test-cluster/node2", 0755)
	os.MkdirAll("/Users/adwaiy/dev/mq/data/test-cluster/node3", 0755)

	// Start 3 brokers
	fmt.Println("Starting 3 brokers...")

	cmd1 := startBroker("1", "9001", "./data/test-cluster/node1", "")
	cmd2 := startBroker("2", "9002", "./data/test-cluster/node2", "1:localhost:9001")
	cmd3 := startBroker("3", "9003", "./data/test-cluster/node3", "1:localhost:9001,2:localhost:9002")

	defer cmd1.Process.Kill()
	defer cmd2.Process.Kill()
	defer cmd3.Process.Kill()

	// Wait for brokers to start
	time.Sleep(3 * time.Second)

	// Connect to broker 1
	fmt.Println("\nConnecting to broker 1...")
	conn, err := grpc.Dial("localhost:9001", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := mqpb.NewMessageQueueClient(conn)
	ctx := context.Background()

	// Create topic
	fmt.Println("Creating topic 'orders' with 3 partitions...")
	_, err = client.CreateTopic(ctx, &mqpb.CreateTopicRequest{
		Topic:      "orders",
		Partitions: 3,
	})
	if err != nil {
		log.Fatalf("CreateTopic failed: %v", err)
	}

	// Produce messages
	fmt.Println("Producing 10 messages...")
	for i := 0; i < 10; i++ {
		partition := int32(i % 3)
		_, err := client.Produce(ctx, &mqpb.ProduceRequest{
			Topic:     "orders",
			Partition: partition,
			Key:       []byte(fmt.Sprintf("key-%d", i)),
			Value:     []byte(fmt.Sprintf("value-%d", i)),
			Acks:      1,
		})
		if err != nil {
			log.Printf("Produce error: %v", err)
		}
	}
	fmt.Println("Messages produced successfully")

	// Consume messages
	fmt.Println("\nConsuming messages...")
	for p := int32(0); p < 3; p++ {
		resp, err := client.Consume(ctx, &mqpb.ConsumeRequest{
			Topic:     "orders",
			Partition: p,
			Offset:    0,
		})
		if err != nil {
			log.Printf("Consume error for partition %d: %v", p, err)
			continue
		}
		fmt.Printf("  Partition %d: %d messages, high watermark %d\n", p, len(resp.Messages), resp.HighWatermark)
	}

	// Test consumer groups across brokers
	fmt.Println("\nTesting consumer group...")
	testConsumerGroup()

	fmt.Println("\n=== Test Complete ===")
}

func startBroker(id, port, dataDir, peers string) *exec.Cmd {
	args := []string{
		"-id=" + id,
		"-port=" + port,
		"-data=" + dataDir,
	}
	if peers != "" {
		args = append(args, "-peers="+peers)
	}

	cmd := exec.Command("./bin/broker", args...)
	cmd.Dir = "/Users/adwaiy/dev/mq"
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		log.Fatalf("Failed to start broker %s: %v", id, err)
	}

	fmt.Printf("  Started broker %s on port %s\n", id, port)
	return cmd
}

func testConsumerGroup() {
	conn, err := grpc.Dial("localhost:9001", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Failed to connect for consumer test: %v", err)
		return
	}
	defer conn.Close()

	client := mqpb.NewMessageQueueClient(conn)
	ctx := context.Background()

	// Join group
	joinResp, err := client.JoinGroup(ctx, &mqpb.JoinGroupRequest{
		GroupId:  "test-group",
		MemberId: "consumer-1",
		Topics:   []string{"orders"},
	})
	if err != nil {
		log.Printf("JoinGroup error: %v", err)
		return
	}
	fmt.Printf("  Joined group as %s (generation %d)\n", joinResp.MemberId, joinResp.GenerationId)

	// Sync group
	syncResp, err := client.SyncGroup(ctx, &mqpb.SyncGroupRequest{
		GroupId:      "test-group",
		MemberId:     "consumer-1",
		GenerationId: joinResp.GenerationId,
	})
	if err != nil {
		log.Printf("SyncGroup error: %v", err)
		return
	}
	fmt.Printf("  Assigned partitions: %v\n", syncResp.Assignment)

	// Leave group
	_, err = client.LeaveGroup(ctx, &mqpb.LeaveGroupRequest{
		GroupId:  "test-group",
		MemberId: "consumer-1",
	})
	if err != nil {
		log.Printf("LeaveGroup error: %v", err)
		return
	}
	fmt.Println("  Left group successfully")
}
