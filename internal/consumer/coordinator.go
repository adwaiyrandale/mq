package consumer

import (
	"sync"
	"time"
)

type GroupState int

const (
	Stable GroupState = iota
	PreparingRebalance
	CompletingRebalance
)

type Member struct {
	MemberID       string
	GroupID        string
	ClientID       string
	ClientHost     string
	Subscription   map[string]int64   // topic -> offset
	Assignment     map[string][]int32 // topic -> assigned partitions
	LastHeartbeat  time.Time
	SessionTimeout time.Duration
}

type Group struct {
	GroupID          string
	Protocol         string
	ProtocolType     string
	State            GroupState
	Members          map[string]*Member
	GenerationID     int32
	LeaderID         string
	mu               sync.RWMutex
	subscribedTopics map[string]struct{}
}

type Coordinator struct {
	groups             map[string]*Group
	members            map[string]*Member
	mu                 sync.RWMutex
	sessionTimeout     time.Duration
	rebalancer         *Rebalancer
	getTopicPartitions func(string) (int32, bool)
}

func NewCoordinator(getTopicPartitions func(string) (int32, bool)) *Coordinator {
	c := &Coordinator{
		groups:             make(map[string]*Group),
		members:            make(map[string]*Member),
		sessionTimeout:     10 * time.Second,
		rebalancer:         NewRebalancer(RoundRobinStrategy),
		getTopicPartitions: getTopicPartitions,
	}
	go c.cleanupExpiredMembers()
	return c
}

func (c *Coordinator) JoinGroup(groupID string, memberID string, topics []string) (string, int32, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	group, exists := c.groups[groupID]
	if !exists {
		group = &Group{
			GroupID:          groupID,
			Members:          make(map[string]*Member),
			GenerationID:     1,
			subscribedTopics: make(map[string]struct{}),
		}
		c.groups[groupID] = group
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	if group.State == Stable {
		group.State = PreparingRebalance
	}

	member := &Member{
		MemberID:       memberID,
		GroupID:        groupID,
		LastHeartbeat:  time.Now(),
		SessionTimeout: c.sessionTimeout,
		Subscription:   make(map[string]int64),
	}

	for _, topic := range topics {
		group.subscribedTopics[topic] = struct{}{}
		member.Subscription[topic] = 0
	}

	group.Members[memberID] = member
	c.members[memberID] = member

	if group.LeaderID == "" {
		group.LeaderID = memberID
	}

	return memberID, group.GenerationID, nil
}

func (c *Coordinator) SyncGroup(groupID string, memberID string, assignment map[string][]int32) (map[string][]int32, error) {
	c.mu.RLock()
	group, exists := c.groups[groupID]
	c.mu.RUnlock()

	if !exists {
		return nil, nil
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	member, exists := group.Members[memberID]
	if !exists {
		return nil, nil
	}

	member.Assignment = assignment

	if group.LeaderID == memberID {
		group.State = Stable
	}

	return member.Assignment, nil
}

func (c *Coordinator) Heartbeat(groupID string, memberID string) error {
	c.mu.RLock()
	group, exists := c.groups[groupID]
	c.mu.RUnlock()

	if !exists {
		return nil
	}

	group.mu.RLock()
	member, exists := group.Members[memberID]
	group.mu.RUnlock()

	if !exists {
		return nil
	}

	member.LastHeartbeat = time.Now()
	return nil
}

func (c *Coordinator) LeaveGroup(groupID string, memberID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	group, exists := c.groups[groupID]
	if !exists {
		return nil
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	delete(group.Members, memberID)
	delete(c.members, memberID)

	if len(group.Members) == 0 {
		delete(c.groups, groupID)
		return nil
	}

	if group.LeaderID == memberID {
		for id := range group.Members {
			group.LeaderID = id
			break
		}
	}

	if group.State == Stable {
		group.State = PreparingRebalance
	}

	return nil
}

func (c *Coordinator) GetAssignment(groupID string, memberID string) (map[string][]int32, bool) {
	c.mu.RLock()
	group, exists := c.groups[groupID]
	c.mu.RUnlock()

	if !exists {
		return nil, false
	}

	group.mu.RLock()
	member, exists := group.Members[memberID]
	group.mu.RUnlock()

	if !exists {
		return nil, false
	}

	return member.Assignment, true
}

func (c *Coordinator) cleanupExpiredMembers() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.Lock()
		for groupID, group := range c.groups {
			group.mu.Lock()
			for memberID, member := range group.Members {
				if time.Since(member.LastHeartbeat) > member.SessionTimeout {
					delete(group.Members, memberID)
					delete(c.members, memberID)
					if group.LeaderID == memberID && len(group.Members) > 0 {
						for id := range group.Members {
							group.LeaderID = id
							break
						}
					}
				}
			}
			if len(group.Members) == 0 {
				delete(c.groups, groupID)
			}
			group.mu.Unlock()
		}
		c.mu.Unlock()
	}
}

func (c *Coordinator) ListGroupMembers(groupID string) []string {
	c.mu.RLock()
	group, exists := c.groups[groupID]
	c.mu.RUnlock()

	if !exists {
		return nil
	}

	group.mu.RLock()
	defer group.mu.RUnlock()

	members := make([]string, 0, len(group.Members))
	for id := range group.Members {
		members = append(members, id)
	}
	return members
}

func (c *Coordinator) PerformRebalance(groupID string) (map[string]map[string][]int32, error) {
	c.mu.RLock()
	group, exists := c.groups[groupID]
	c.mu.RUnlock()

	if !exists {
		return nil, nil
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	memberIDs := make([]string, 0, len(group.Members))
	for id := range group.Members {
		memberIDs = append(memberIDs, id)
	}

	if len(memberIDs) == 0 {
		return nil, nil
	}

	// Build topic partition counts from subscribed topics
	topicPartitions := make(map[string]int32)
	for topic := range group.subscribedTopics {
		if partitionCount, ok := c.getTopicPartitions(topic); ok {
			topicPartitions[topic] = partitionCount
		}
	}

	// Use rebalancer to assign partitions
	assignments := c.rebalancer.Assign(memberIDs, topicPartitions)

	// Clear old assignments and set new ones
	result := make(map[string]map[string][]int32)
	for memberID, parts := range assignments {
		member := group.Members[memberID]
		member.Assignment = make(map[string][]int32)
		result[memberID] = make(map[string][]int32)

		for _, p := range parts {
			member.Assignment[p.Topic] = append(member.Assignment[p.Topic], p.Partition)
			result[memberID][p.Topic] = append(result[memberID][p.Topic], p.Partition)
		}
	}

	group.State = Stable
	group.GenerationID++

	return result, nil
}
