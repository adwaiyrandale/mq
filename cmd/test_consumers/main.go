package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/adwaiy/mq/client"
	mqpb "github.com/adwaiy/mq/proto"
)

func produceMessages(brokerAddr string, topic string, count int) {
	conn, err := grpc.Dial(brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := mqpb.NewMessageQueueClient(conn)
	ctx := context.Background()

	// Create topic if not exists
	client.CreateTopic(ctx, &mqpb.CreateTopicRequest{
		Topic:      topic,
		Partitions: 3,
	})

	fmt.Printf("Producing %d messages to %s...\n", count, topic)
	for i := 0; i < count; i++ {
		partition := int32(i % 3)
		_, err := client.Produce(ctx, &mqpb.ProduceRequest{
			Topic:     topic,
			Partition: partition,
			Key:       []byte(fmt.Sprintf("key-%d", i)),
			Value:     []byte(fmt.Sprintf("message-%d", i)),
		})
		if err != nil {
			log.Printf("Produce error: %v", err)
		}
	}
	fmt.Println("Done producing")
}

func createConsumer(consumerNum int, groupID string, topics []string, brokerAddr string, wg *sync.WaitGroup) *client.Consumer {
	c, err := client.NewConsumer(client.ConsumerConfig{
		GroupID:    groupID,
		Topics:     topics,
		BrokerAddr: brokerAddr,
		AutoCommit: true,
	})
	if err != nil {
		log.Fatalf("Consumer %d: Failed to create: %v", consumerNum, err)
	}

	msgCount := 0
	err = c.Subscribe(func(msg *mqpb.Message) {
		msgCount++
		if msgCount <= 3 {
			fmt.Printf("Consumer %d [%s]: topic=%s partition=%d offset=%d key=%s value=%s\n",
				consumerNum, groupID, msg.Topic, msg.Partition, msg.Offset,
				string(msg.Key), string(msg.Value))
		}
	})

	if err != nil {
		log.Fatalf("Consumer %d: Failed to subscribe: %v", consumerNum, err)
	}

	assignment := c.Assignment()
	fmt.Printf("Consumer %d [%s] assigned: %v\n", consumerNum, groupID, assignment)

	return c
}

func main() {
	brokerAddr := "localhost:9001"
	topic := "test-topic"
	groupID := "test-group"

	// Produce some messages first
	produceMessages(brokerAddr, topic, 30)

	fmt.Println("\n=== Test: Multiple Consumers in Same Group ===\n")

	var wg sync.WaitGroup
	consumers := make([]*client.Consumer, 0)

	// Start first consumer
	fmt.Println("Starting Consumer 1...")
	c1 := createConsumer(1, groupID, []string{topic}, brokerAddr, &wg)
	consumers = append(consumers, c1)
	time.Sleep(2 * time.Second)

	// Start second consumer - should trigger rebalancing
	fmt.Println("\nStarting Consumer 2 (should trigger rebalance)...")
	c2 := createConsumer(2, groupID, []string{topic}, brokerAddr, &wg)
	consumers = append(consumers, c2)
	time.Sleep(2 * time.Second)

	// Start third consumer - should trigger another rebalance
	fmt.Println("\nStarting Consumer 3 (should trigger rebalance)...")
	c3 := createConsumer(3, groupID, []string{topic}, brokerAddr, &wg)
	consumers = append(consumers, c3)

	// Let consumers run for a bit
	fmt.Println("\nConsuming for 5 seconds...")
	time.Sleep(5 * time.Second)

	// Close consumers
	fmt.Println("\nClosing consumers...")
	for i, c := range consumers {
		fmt.Printf("Closing consumer %d...\n", i+1)
		c.Close()
	}

	fmt.Println("\nDone!")
}
