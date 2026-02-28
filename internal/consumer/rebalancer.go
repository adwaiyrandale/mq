package consumer

import (
	"sort"
)

type AssignmentStrategy string

const (
	RangeStrategy      AssignmentStrategy = "range"
	RoundRobinStrategy AssignmentStrategy = "roundrobin"
)

type Partition struct {
	Topic     string
	Partition int32
}

type MemberAssignment struct {
	MemberID   string
	Partitions []Partition
}

type Rebalancer struct {
	strategy AssignmentStrategy
}

func NewRebalancer(strategy AssignmentStrategy) *Rebalancer {
	return &Rebalancer{strategy: strategy}
}

func (r *Rebalancer) Assign(members []string, topicPartitions map[string]int32) map[string][]Partition {
	switch r.strategy {
	case RangeStrategy:
		return r.assignRange(members, topicPartitions)
	case RoundRobinStrategy:
		return r.assignRoundRobin(members, topicPartitions)
	default:
		return r.assignRoundRobin(members, topicPartitions)
	}
}

func (r *Rebalancer) assignRange(members []string, topicPartitions map[string]int32) map[string][]Partition {
	assignments := make(map[string][]Partition)
	for _, member := range members {
		assignments[member] = []Partition{}
	}

	if len(members) == 0 {
		return assignments
	}

	sortedTopics := make([]string, 0, len(topicPartitions))
	for topic := range topicPartitions {
		sortedTopics = append(sortedTopics, topic)
	}
	sort.Strings(sortedTopics)

	memberIdx := 0
	for _, topic := range sortedTopics {
		partitionCount := topicPartitions[topic]
		partitionsPerMember := int(partitionCount) / len(members)
		extraPartitions := int(partitionCount) % len(members)

		currentPartition := int32(0)
		for i, member := range members {
			partitionsForThisMember := partitionsPerMember
			if i < extraPartitions {
				partitionsForThisMember++
			}

			for j := 0; j < partitionsForThisMember; j++ {
				assignments[member] = append(assignments[member], Partition{
					Topic:     topic,
					Partition: currentPartition,
				})
				currentPartition++
			}
		}
		memberIdx++
	}

	return assignments
}

func (r *Rebalancer) assignRoundRobin(members []string, topicPartitions map[string]int32) map[string][]Partition {
	assignments := make(map[string][]Partition)
	for _, member := range members {
		assignments[member] = []Partition{}
	}

	if len(members) == 0 {
		return assignments
	}

	sortedTopics := make([]string, 0, len(topicPartitions))
	for topic := range topicPartitions {
		sortedTopics = append(sortedTopics, topic)
	}
	sort.Strings(sortedTopics)

	memberIdx := 0
	for _, topic := range sortedTopics {
		for p := int32(0); p < topicPartitions[topic]; p++ {
			member := members[memberIdx%len(members)]
			assignments[member] = append(assignments[member], Partition{
				Topic:     topic,
				Partition: p,
			})
			memberIdx++
		}
	}

	return assignments
}
