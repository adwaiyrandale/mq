package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	mqpb "github.com/adwaiy/mq/proto"
)

type BenchmarkResult struct {
	Name         string
	MessagesSent int64
	MessagesRecv int64
	BytesSent    int64
	BytesRecv    int64
	Duration     time.Duration
	Latencies    []time.Duration
	Errors       int64
}

func (r *BenchmarkResult) Throughput() float64 {
	return float64(r.MessagesSent) / r.Duration.Seconds()
}

func (r *BenchmarkResult) BandwidthMB() float64 {
	return float64(r.BytesSent) / 1024 / 1024 / r.Duration.Seconds()
}

func (r *BenchmarkResult) LatencyAvg() time.Duration {
	if len(r.Latencies) == 0 {
		return 0
	}
	var sum time.Duration
	for _, l := range r.Latencies {
		sum += l
	}
	return sum / time.Duration(len(r.Latencies))
}

func (r *BenchmarkResult) LatencyP50() time.Duration {
	return r.percentile(0.5)
}

func (r *BenchmarkResult) LatencyP99() time.Duration {
	return r.percentile(0.99)
}

func (r *BenchmarkResult) percentile(p float64) time.Duration {
	if len(r.Latencies) == 0 {
		return 0
	}
	sort.Slice(r.Latencies, func(i, j int) bool {
		return r.Latencies[i] < r.Latencies[j]
	})
	idx := int(math.Ceil(float64(len(r.Latencies))*p)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(r.Latencies) {
		idx = len(r.Latencies) - 1
	}
	return r.Latencies[idx]
}

type BenchmarkConfig struct {
	Brokers      []string
	Topic        string
	Partitions   int32
	MessageSize  int
	MessageCount int
	Concurrency  int
	Acks         int32
	Duration     time.Duration
	BatchSize    int
}

func main() {
	var (
		brokers      = flag.String("brokers", "localhost:9001", "Comma-separated broker addresses")
		topic        = flag.String("topic", "benchmark-topic", "Topic name")
		partitions   = flag.Int("partitions", 3, "Number of partitions")
		messageSize  = flag.Int("size", 1024, "Message size in bytes")
		messageCount = flag.Int("count", 100000, "Number of messages")
		concurrency  = flag.Int("concurrency", 10, "Number of concurrent producers/consumers")
		acks         = flag.Int("acks", 1, "Acks level (0, 1, -1)")
		duration     = flag.Duration("duration", 0, "Duration (0 = use message count)")
	)
	flag.Parse()

	config := BenchmarkConfig{
		Brokers:      parseBrokers(*brokers),
		Topic:        *topic,
		Partitions:   int32(*partitions),
		MessageSize:  *messageSize,
		MessageCount: *messageCount,
		Concurrency:  *concurrency,
		Acks:         int32(*acks),
		Duration:     *duration,
	}

	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println("                    DMQ BENCHMARK SUITE                        ")
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Printf("Configuration:\n")
	fmt.Printf("  Brokers:      %v\n", config.Brokers)
	fmt.Printf("  Topic:        %s\n", config.Topic)
	fmt.Printf("  Partitions:   %d\n", config.Partitions)
	fmt.Printf("  Message Size: %d bytes\n", config.MessageSize)
	fmt.Printf("  Message Count: %d\n", config.MessageCount)
	fmt.Printf("  Concurrency:  %d\n", config.Concurrency)
	fmt.Printf("  Acks:         %d\n", config.Acks)
	fmt.Println()

	// Setup - create topic
	if err := setupTopic(config); err != nil {
		log.Printf("Warning: setup failed: %v", err)
	}

	// Run benchmarks
	results := []*BenchmarkResult{}

	// Producer benchmarks
	fmt.Println("\n[PRODUCER BENCHMARKS]")
	for _, acks := range []int32{0, 1, -1} {
		config.Acks = acks
		result := benchmarkProducer(config)
		results = append(results, result)
		printResult(result)
	}

	// Consumer benchmark
	fmt.Println("\n[CONSUMER BENCHMARK]")
	consumerResult := benchmarkConsumer(config)
	results = append(results, consumerResult)
	printResult(consumerResult)

	// End-to-end latency benchmark
	fmt.Println("\n[END-TO-END LATENCY BENCHMARK]")
	latencyResult := benchmarkLatency(config)
	results = append(results, latencyResult)
	printResult(latencyResult)

	// Summary
	fmt.Println("\n═══════════════════════════════════════════════════════════════")
	fmt.Println("                      BENCHMARK SUMMARY                        ")
	fmt.Println("═══════════════════════════════════════════════════════════════")
	for _, r := range results {
		fmt.Printf("%-30s %12.0f msg/s  %8s p99\n", r.Name, r.Throughput(), r.LatencyP99())
	}
}

func parseBrokers(s string) []string {
	var brokers []string
	for _, b := range splitAndTrim(s, ",") {
		brokers = append(brokers, b)
	}
	return brokers
}

func splitAndTrim(s, sep string) []string {
	var result []string
	for _, part := range split(s, sep) {
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}

func split(s, sep string) []string {
	var result []string
	start := 0
	for i := 0; i < len(s); i++ {
		if i < len(s)-len(sep)+1 && s[i:i+len(sep)] == sep {
			result = append(result, s[start:i])
			start = i + len(sep)
			i += len(sep) - 1
		}
	}
	result = append(result, s[start:])
	return result
}

func setupTopic(config BenchmarkConfig) error {
	conn, err := grpc.Dial(config.Brokers[0], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	client := mqpb.NewMessageQueueClient(conn)
	ctx := context.Background()

	_, err = client.CreateTopic(ctx, &mqpb.CreateTopicRequest{
		Topic:      config.Topic,
		Partitions: config.Partitions,
	})
	return err
}

func benchmarkProducer(config BenchmarkConfig) *BenchmarkResult {
	result := &BenchmarkResult{
		Name:      fmt.Sprintf("Producer (acks=%d)", config.Acks),
		Latencies: make([]time.Duration, 0),
	}

	var wg sync.WaitGroup
	start := time.Now()

	messagesPerWorker := config.MessageCount / config.Concurrency

	for i := 0; i < config.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			conn, err := grpc.Dial(config.Brokers[workerID%len(config.Brokers)], grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("Worker %d: connection failed: %v", workerID, err)
				return
			}
			defer conn.Close()

			client := mqpb.NewMessageQueueClient(conn)
			ctx := context.Background()

			payload := make([]byte, config.MessageSize)
			for j := 0; j < len(payload); j++ {
				payload[j] = byte(j % 256)
			}

			for j := 0; j < messagesPerWorker; j++ {
				partition := int32(j % int(config.Partitions))

				msgStart := time.Now()
				_, err := client.Produce(ctx, &mqpb.ProduceRequest{
					Topic:     config.Topic,
					Partition: partition,
					Key:       []byte(fmt.Sprintf("key-%d-%d", workerID, j)),
					Value:     payload,
					Acks:      config.Acks,
				})
				latency := time.Since(msgStart)

				if err != nil {
					atomic.AddInt64(&result.Errors, 1)
				} else {
					atomic.AddInt64(&result.MessagesSent, 1)
					atomic.AddInt64(&result.BytesSent, int64(config.MessageSize))
					result.Latencies = append(result.Latencies, latency)
				}
			}
		}(i)
	}

	wg.Wait()
	result.Duration = time.Since(start)

	return result
}

func benchmarkConsumer(config BenchmarkConfig) *BenchmarkResult {
	result := &BenchmarkResult{
		Name:      "Consumer",
		Latencies: make([]time.Duration, 0),
	}

	// First produce some messages
	fmt.Printf("  Pre-populating %d messages...\n", config.MessageCount)
	produceForConsumer(config)

	var wg sync.WaitGroup
	start := time.Now()
	done := make(chan struct{})

	var totalConsumed int64

	for i := 0; i < config.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			conn, err := grpc.Dial(config.Brokers[workerID%len(config.Brokers)], grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return
			}
			defer conn.Close()

			client := mqpb.NewMessageQueueClient(conn)
			ctx := context.Background()

			for {
				select {
				case <-done:
					return
				default:
				}

				for p := int32(0); p < config.Partitions; p++ {
					resp, err := client.Consume(ctx, &mqpb.ConsumeRequest{
						Topic:     config.Topic,
						Partition: p,
						Offset:    0,
						MaxBytes:  1024 * 1024,
					})
					if err == nil && len(resp.Messages) > 0 {
						atomic.AddInt64(&totalConsumed, int64(len(resp.Messages)))
						atomic.AddInt64(&result.BytesRecv, int64(len(resp.Messages)*config.MessageSize))
					}
				}

				if atomic.LoadInt64(&totalConsumed) >= int64(config.MessageCount) {
					return
				}
			}
		}(i)
	}

	time.Sleep(5 * time.Second)
	close(done)
	wg.Wait()

	result.Duration = time.Since(start)
	result.MessagesRecv = totalConsumed

	return result
}

func produceForConsumer(config BenchmarkConfig) {
	conn, _ := grpc.Dial(config.Brokers[0], grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer conn.Close()
	client := mqpb.NewMessageQueueClient(conn)
	ctx := context.Background()

	payload := make([]byte, config.MessageSize)
	for i := 0; i < config.MessageCount/10; i++ {
		partition := int32(i % int(config.Partitions))
		client.Produce(ctx, &mqpb.ProduceRequest{
			Topic:     config.Topic,
			Partition: partition,
			Key:       []byte(fmt.Sprintf("key-%d", i)),
			Value:     payload,
			Acks:      1,
		})
	}
}

func benchmarkLatency(config BenchmarkConfig) *BenchmarkResult {
	result := &BenchmarkResult{
		Name:      "End-to-End Latency",
		Latencies: make([]time.Duration, 0),
	}

	conn, _ := grpc.Dial(config.Brokers[0], grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer conn.Close()
	client := mqpb.NewMessageQueueClient(conn)
	ctx := context.Background()

	payload := make([]byte, 100) // Small payload for latency test
	count := 1000

	start := time.Now()
	for i := 0; i < count; i++ {
		msgStart := time.Now()

		// Produce
		_, err := client.Produce(ctx, &mqpb.ProduceRequest{
			Topic:     config.Topic,
			Partition: 0,
			Key:       []byte(fmt.Sprintf("lat-key-%d", i)),
			Value:     payload,
			Acks:      1,
		})
		if err != nil {
			continue
		}

		// Immediate consume (in real test, consumer would be separate)
		_, _ = client.Consume(ctx, &mqpb.ConsumeRequest{
			Topic:     config.Topic,
			Partition: 0,
			Offset:    int64(i),
			MaxBytes:  1024,
		})

		result.Latencies = append(result.Latencies, time.Since(msgStart))
	}

	result.Duration = time.Since(start)
	result.MessagesSent = int64(count)

	return result
}

func printResult(r *BenchmarkResult) {
	fmt.Printf("\n  %s\n", r.Name)
	fmt.Printf("    Messages:      %d\n", r.MessagesSent+r.MessagesRecv)
	fmt.Printf("    Duration:      %v\n", r.Duration)
	fmt.Printf("    Throughput:    %.0f msg/s\n", r.Throughput())
	fmt.Printf("    Bandwidth:     %.2f MB/s\n", r.BandwidthMB())
	if len(r.Latencies) > 0 {
		fmt.Printf("    Latency (avg): %v\n", r.LatencyAvg())
		fmt.Printf("    Latency (p50): %v\n", r.LatencyP50())
		fmt.Printf("    Latency (p99): %v\n", r.LatencyP99())
	}
	if r.Errors > 0 {
		fmt.Printf("    Errors:        %d\n", r.Errors)
	}
}
