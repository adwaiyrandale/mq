package main

import (
	"fmt"
	"time"

	"github.com/adwaiy/mq/internal/raft"
)

func main() {
	fmt.Println("=== Raft Consensus Test ===\n")

	// Create 3 nodes
	node1 := raft.NewNode("node1", []string{"node2", "node3"})
	node2 := raft.NewNode("node2", []string{"node1", "node3"})
	node3 := raft.NewNode("node3", []string{"node1", "node2"})

	// Set up communication between nodes
	nodes := map[string]*raft.Node{
		"node1": node1,
		"node2": node2,
		"node3": node3,
	}

	// Set up send functions
	for id, node := range nodes {
		node.SendRequestVote = func(peer string, args raft.RequestVoteArgs) error {
			if target, ok := nodes[peer]; ok {
				reply := target.HandleRequestVote(args)
				// Process reply on the sender
				if reply.VoteGranted && nodes[args.CandidateID].State() == raft.Candidate {
					// Count votes (simplified - just become leader if majority)
					if args.Term >= nodes[args.CandidateID].Term() {
						nodes[args.CandidateID].BecomeLeader()
					}
				}
			}
			return nil
		}

		node.SendAppendEntries = func(peer string, args raft.AppendEntriesArgs) error {
			if target, ok := nodes[peer]; ok {
				reply := target.HandleAppendEntries(args)
				if reply.Success {
					// Update match index
					if node.State() == raft.Leader {
						// In real implementation, track replication
					}
				}
			}
			return nil
		}

		_ = id
	}

	// Start all nodes
	fmt.Println("Starting 3 nodes...")
	for id, node := range nodes {
		go node.Run()
		fmt.Printf("  Started %s as Follower\n", id)
	}

	// Wait for leader election
	fmt.Println("\nWaiting for leader election (3 seconds)...")
	time.Sleep(3 * time.Second)

	// Check who is leader
	fmt.Println("\n--- Leader Election Results ---")
	leaderCount := 0
	for id, node := range nodes {
		state := node.State()
		term := node.Term()
		fmt.Printf("  %s: %s (term %d)\n", id, state, term)
		if state == raft.Leader {
			leaderCount++
		}
	}

	if leaderCount == 1 {
		fmt.Println("  ✓ Exactly one leader elected (correct)")
	} else {
		fmt.Printf("  ✗ Expected 1 leader, got %d\n", leaderCount)
	}

	// Test log replication
	fmt.Println("\n--- Log Replication Test ---")

	// Find leader and propose a command
	for id, node := range nodes {
		if node.State() == raft.Leader {
			fmt.Printf("  Proposing command on leader %s...\n", id)
			success := node.Propose("test-command")
			if success {
				fmt.Println("  ✓ Command proposed successfully")
			} else {
				fmt.Println("  ✗ Failed to propose command")
			}
			break
		}
	}

	time.Sleep(1 * time.Second)

	// Check logs
	fmt.Println("\n--- Log Status ---")
	for id, node := range nodes {
		// Access log through the node's methods
		fmt.Printf("  %s log entries: %d\n", id, len(node.Log))
	}

	fmt.Println("\nDone!")
}
