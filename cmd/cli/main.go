package main

import (
	"context"
	"fmt"
	"io"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	mqpb "github.com/adwaiy/mq/proto"
)

func main() {
	conn, err := grpc.Dial(":9001", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := mqpb.NewMessageQueueClient(conn)
	ctx := context.Background()

	// Create test topic
	fmt.Println("Creating topic 'orders'...")
	_, err = client.CreateTopic(ctx, &mqpb.CreateTopicRequest{
		Topic:      "orders",
		Partitions: 3,
	})
	if err != nil {
		log.Printf("CreateTopic error: %v", err)
	}

	// Produce some messages
	fmt.Println("\nProducing messages to 'orders'...")
	for i := 0; i < 10; i++ {
		_, err := client.Produce(ctx, &mqpb.ProduceRequest{
			Topic:     "orders",
			Key:       []byte(fmt.Sprintf("key-%d", i)),
			Value:     []byte(fmt.Sprintf("order-data-%d", i)),
			Partition: 0,
		})
		if err != nil {
			log.Printf("Produce error: %v", err)
		}
	}
	fmt.Println("Produced 10 messages")

	// Join a consumer group
	fmt.Println("\n--- Consumer Group Test ---")
	fmt.Println("Joining consumer group 'order-processor'...")

	memberID := fmt.Sprintf("consumer-%d", 0)
	joinResp, err := client.JoinGroup(ctx, &mqpb.JoinGroupRequest{
		GroupId:  "order-processor",
		MemberId: memberID,
		Topics:   []string{"orders"},
	})
	if err != nil {
		log.Fatalf("JoinGroup error: %v", err)
	}
	fmt.Printf("Joined group as member: %s, generation: %d\n", joinResp.MemberId, joinResp.GenerationId)
	fmt.Printf("Group members: %v\n", joinResp.Members)

	// Sync group to get assignment
	fmt.Println("\nSyncing group...")
	syncResp, err := client.SyncGroup(ctx, &mqpb.SyncGroupRequest{
		GroupId:      "order-processor",
		MemberId:     memberID,
		GenerationId: joinResp.GenerationId,
	})
	if err != nil {
		log.Fatalf("SyncGroup error: %v", err)
	}
	fmt.Printf("Sync success: %v, assignment: %v\n", syncResp.Success, syncResp.Assignment)

	// Subscribe to messages
	fmt.Println("\nSubscribing to messages (streaming)...")
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	subReq := &mqpb.SubscribeRequest{
		GroupId:     "order-processor",
		Topic:       "orders",
		Partition:   0,
		StartOffset: 0,
	}

	stream, err := client.Subscribe(subCtx, subReq)
	if err != nil {
		log.Fatalf("Subscribe error: %v", err)
	}

	msgCount := 0
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Stream error: %v", err)
			break
		}

		fmt.Printf("  Received: offset=%d, key=%s, value=%s\n",
			msg.Offset, string(msg.Key), string(msg.Value))
		msgCount++

		if msgCount >= 5 {
			fmt.Println("\nCommitting offset...")
			commitResp, err := client.CommitOffset(ctx, &mqpb.CommitOffsetRequest{
				GroupId:   "order-processor",
				Topic:     "orders",
				Partition: 0,
				Offset:    msg.Offset,
			})
			if err != nil {
				log.Printf("CommitOffset error: %v", err)
			} else {
				fmt.Printf("Committed offset %d: success=%v\n", msg.Offset, commitResp.Success)
			}
			break
		}
	}

	// Leave group
	fmt.Println("\nLeaving group...")
	leaveResp, err := client.LeaveGroup(ctx, &mqpb.LeaveGroupRequest{
		GroupId:  "order-processor",
		MemberId: memberID,
	})
	if err != nil {
		log.Printf("LeaveGroup error: %v", err)
	} else {
		fmt.Printf("Left group: success=%v\n", leaveResp.Success)
	}

	fmt.Println("\nDone!")
}
