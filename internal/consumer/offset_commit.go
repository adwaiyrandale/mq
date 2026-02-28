package consumer

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/adwaiy/mq/internal/storage"
)

type OffsetCommit struct {
	storage *storage.WAL
}

func NewOffsetCommit(storage *storage.WAL) *OffsetCommit {
	return &OffsetCommit{storage: storage}
}

func (oc *OffsetCommit) CommitOffset(groupID string, topic string, partition int32, offset int64) error {
	key := fmt.Sprintf("%s|%s|%d", groupID, topic, partition)
	value := fmt.Sprintf("%s|%s|%d|%d", groupID, topic, partition, offset)

	record := &storage.Record{
		Key:       []byte(key),
		Value:     []byte(value),
		Timestamp: 0,
	}

	offsetsPartition, err := oc.storage.GetPartition("__consumer_offsets", 0)
	if err != nil {
		return fmt.Errorf("__consumer_offsets partition not found: %v", err)
	}

	_, err = offsetsPartition.Append(record)
	return err
}

func (oc *OffsetCommit) GetCommittedOffset(groupID string, topic string, partition int32) (int64, bool, error) {
	key := fmt.Sprintf("%s|%s|%d", groupID, topic, partition)

	offsetsPartition, err := oc.storage.GetPartition("__consumer_offsets", 0)
	if err != nil {
		return 0, false, fmt.Errorf("__consumer_offsets partition not found: %v", err)
	}

	var latestOffset int64 = -1
	var found bool

	for i := int64(0); i < offsetsPartition.LogEndOffset; i++ {
		record, err := offsetsPartition.Read(i)
		if err != nil {
			continue
		}

		if string(record.Key) == key {
			parts := strings.Split(string(record.Value), "|")
			if len(parts) == 4 {
				offset, err := strconv.ParseInt(parts[3], 10, 64)
				if err == nil {
					latestOffset = offset
					found = true
				}
			}
		}
	}

	return latestOffset, found, nil
}

func (oc *OffsetCommit) GetCommittedOffsetsForGroup(groupID string) (map[string]map[int32]int64, error) {
	result := make(map[string]map[int32]int64)

	offsetsPartition, err := oc.storage.GetPartition("__consumer_offsets", 0)
	if err != nil {
		return nil, fmt.Errorf("__consumer_offsets partition not found: %v", err)
	}

	for i := int64(0); i < offsetsPartition.LogEndOffset; i++ {
		record, err := offsetsPartition.Read(i)
		if err != nil {
			continue
		}

		parts := strings.Split(string(record.Value), "|")
		if len(parts) != 4 {
			continue
		}

		recordGroupID := parts[0]
		if recordGroupID != groupID {
			continue
		}

		topic := parts[1]
		partition, err := strconv.ParseInt(parts[2], 10, 32)
		if err != nil {
			continue
		}
		offset, err := strconv.ParseInt(parts[3], 10, 64)
		if err != nil {
			continue
		}

		if result[topic] == nil {
			result[topic] = make(map[int32]int64)
		}
		result[topic][int32(partition)] = offset
	}

	return result, nil
}
