package broker

import (
	"sync"
	"time"
)

// Replica tracks the state of a follower replica
type Replica struct {
	BrokerID      string
	FetchOffset   int64
	LastFetchTime time.Time
	IsInISR       bool
}

// PartitionReplication manages replication for a partition
type PartitionReplication struct {
	Topic       string
	PartitionID int32
	LeaderID    string
	Replicas    map[string]*Replica // broker ID -> replica
	ISR         map[string]bool     // broker IDs in ISR
	MinISR      int
	mu          sync.RWMutex
}

func NewPartitionReplication(topic string, partitionID int32, leaderID string, replicaIDs []string, minISR int) *PartitionReplication {
	pr := &PartitionReplication{
		Topic:       topic,
		PartitionID: partitionID,
		LeaderID:    leaderID,
		Replicas:    make(map[string]*Replica),
		ISR:         make(map[string]bool),
		MinISR:      minISR,
	}

	for _, id := range replicaIDs {
		pr.Replicas[id] = &Replica{
			BrokerID:      id,
			FetchOffset:   0,
			LastFetchTime: time.Now(),
			IsInISR:       id == leaderID, // Leader is always in ISR
		}
		if id == leaderID {
			pr.ISR[id] = true
		}
	}

	return pr
}

func (pr *PartitionReplication) UpdateFetchOffset(brokerID string, offset int64) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if replica, ok := pr.Replicas[brokerID]; ok {
		replica.FetchOffset = offset
		replica.LastFetchTime = time.Now()

		// Add to ISR if catching up
		if !replica.IsInISR && pr.isCaughtUp(replica) {
			replica.IsInISR = true
			pr.ISR[brokerID] = true
		}
	}
}

func (pr *PartitionReplication) isCaughtUp(replica *Replica) bool {
	// For simplicity, consider caught up if fetched in last 10 seconds
	return time.Since(replica.LastFetchTime) < 10*time.Second
}

func (pr *PartitionReplication) RemoveFromISR(brokerID string) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if brokerID == pr.LeaderID {
		return // Never remove leader from ISR
	}

	if replica, ok := pr.Replicas[brokerID]; ok {
		replica.IsInISR = false
		delete(pr.ISR, brokerID)
	}
}

func (pr *PartitionReplication) GetISR() []string {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	isr := make([]string, 0, len(pr.ISR))
	for id := range pr.ISR {
		isr = append(isr, id)
	}
	return isr
}

func (pr *PartitionReplication) IsISRQuorum() bool {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	return len(pr.ISR) >= pr.MinISR
}

func (pr *PartitionReplication) GetHighWatermark() int64 {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	// High watermark is the minimum fetch offset across all ISR replicas
	var minOffset int64 = -1
	for id := range pr.ISR {
		if replica, ok := pr.Replicas[id]; ok {
			if minOffset == -1 || replica.FetchOffset < minOffset {
				minOffset = replica.FetchOffset
			}
		}
	}
	return minOffset
}

// Replicator manages replication for all partitions
type Replicator struct {
	partitions map[string]*PartitionReplication // "topic-partition" -> replication
	mu         sync.RWMutex
}

func NewReplicator() *Replicator {
	return &Replicator{
		partitions: make(map[string]*PartitionReplication),
	}
}

func (r *Replicator) AddPartition(topic string, partitionID int32, leaderID string, replicaIDs []string, minISR int) {
	key := partitionKey(topic, partitionID)
	r.mu.Lock()
	defer r.mu.Unlock()

	r.partitions[key] = NewPartitionReplication(topic, partitionID, leaderID, replicaIDs, minISR)
}

func (r *Replicator) GetPartition(topic string, partitionID int32) (*PartitionReplication, bool) {
	key := partitionKey(topic, partitionID)
	r.mu.RLock()
	defer r.mu.RUnlock()

	pr, ok := r.partitions[key]
	return pr, ok
}

func (r *Replicator) UpdateFetchOffset(topic string, partitionID int32, brokerID string, offset int64) {
	if pr, ok := r.GetPartition(topic, partitionID); ok {
		pr.UpdateFetchOffset(brokerID, offset)
	}
}

func (r *Replicator) IsISRQuorum(topic string, partitionID int32) bool {
	if pr, ok := r.GetPartition(topic, partitionID); ok {
		return pr.IsISRQuorum()
	}
	return false
}

func partitionKey(topic string, partitionID int32) string {
	return topic + "-" + string(rune(partitionID))
}
