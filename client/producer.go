package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	mqpb "github.com/adwaiy/mq/proto"
)

// ProducerConfig configures the producer
type ProducerConfig struct {
	BrokerAddr        string
	BatchSize         int
	LingerMs          time.Duration
	Retries           int
	Acks              int32 // 0, 1, or -1
	EnableIdempotence bool
}

// Producer produces messages to the message queue
type Producer struct {
	conn    *grpc.ClientConn
	client  mqpb.MessageQueueClient
	config  ProducerConfig
	mu      sync.Mutex
	batch   []*mqpb.ProduceRequest
	timer   *time.Timer
	batchCh chan []*mqpb.ProduceRequest
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// NewProducer creates a new producer
func NewProducer(config ProducerConfig) (*Producer, error) {
	conn, err := grpc.Dial(config.BrokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %v", err)
	}

	if config.BatchSize == 0 {
		config.BatchSize = 100
	}
	if config.LingerMs == 0 {
		config.LingerMs = 10 * time.Millisecond
	}
	if config.Retries == 0 {
		config.Retries = 3
	}
	if config.Acks == 0 {
		config.Acks = 1
	}

	p := &Producer{
		conn:    conn,
		client:  mqpb.NewMessageQueueClient(conn),
		config:  config,
		batch:   make([]*mqpb.ProduceRequest, 0, config.BatchSize),
		batchCh: make(chan []*mqpb.ProduceRequest, 10),
		stopCh:  make(chan struct{}),
	}

	p.wg.Add(1)
	go p.batchProcessor()

	return p, nil
}

// Send sends a message synchronously
func (p *Producer) Send(topic string, key, value []byte, headers map[string]string) (*mqpb.ProduceResponse, error) {
	req := &mqpb.ProduceRequest{
		Topic:   topic,
		Key:     key,
		Value:   value,
		Headers: headers,
		Acks:    p.config.Acks,
	}

	return p.client.Produce(context.Background(), req)
}

// SendAsync sends a message asynchronously (batched)
func (p *Producer) SendAsync(topic string, key, value []byte, headers map[string]string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	req := &mqpb.ProduceRequest{
		Topic:   topic,
		Key:     key,
		Value:   value,
		Headers: headers,
		Acks:    p.config.Acks,
	}

	p.batch = append(p.batch, req)

	// Start timer on first message
	if len(p.batch) == 1 {
		p.timer = time.AfterFunc(p.config.LingerMs, p.flushBatch)
	}

	// Flush if batch is full
	if len(p.batch) >= p.config.BatchSize {
		if p.timer != nil {
			p.timer.Stop()
		}
		p.flushBatch()
	}

	return nil
}

func (p *Producer) flushBatch() {
	p.mu.Lock()
	if len(p.batch) == 0 {
		p.mu.Unlock()
		return
	}
	batch := p.batch
	p.batch = make([]*mqpb.ProduceRequest, 0, p.config.BatchSize)
	p.mu.Unlock()

	select {
	case p.batchCh <- batch:
	case <-p.stopCh:
	}
}

func (p *Producer) batchProcessor() {
	defer p.wg.Done()

	for {
		select {
		case <-p.stopCh:
			return
		case batch := <-p.batchCh:
			p.sendBatch(batch)
		}
	}
}

func (p *Producer) sendBatch(batch []*mqpb.ProduceRequest) {
	ctx := context.Background()

	for _, req := range batch {
		var err error
		for i := 0; i < p.config.Retries; i++ {
			_, err = p.client.Produce(ctx, req)
			if err == nil {
				break
			}
			time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
		}
		if err != nil {
			// In production, send to error handler
			fmt.Printf("Failed to produce after %d retries: %v\n", p.config.Retries, err)
		}
	}
}

// Flush flushes any pending messages
func (p *Producer) Flush() {
	p.mu.Lock()
	if len(p.batch) > 0 {
		if p.timer != nil {
			p.timer.Stop()
		}
		p.flushBatch()
	}
	p.mu.Unlock()
}

// Close closes the producer
func (p *Producer) Close() error {
	p.Flush()
	close(p.stopCh)
	p.wg.Wait()
	return p.conn.Close()
}
