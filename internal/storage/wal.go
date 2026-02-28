package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/adwaiy/mq/pkg/codec"
)

var (
	ErrOffsetOutOfRange = errors.New("offset out of range")
	ErrNotFound         = errors.New("record not found")
	ErrCompacted        = errors.New("offset was compacted")
	ErrSegmentNotFound  = errors.New("segment not found")
)

const (
	HeaderSize          = 21
	RecordOverhead      = 4 + 1 + 8 + 4 + 4
	IndexEntrySize      = 12
	TimeIndexEntrySize  = 12
	DefaultSegmentSize  = int64(1024 * 1024 * 1024)
	DefaultMaxIndexSize = int64(1024 * 1024 * 10)
)

type Config struct {
	DataDir           string
	SegmentSize       int64
	IndexInterval     int
	RetentionMs       int64
	RetentionBytes    int64
	CompactionEnabled bool
}

type TopicConfig struct {
	RetentionMs    int64
	RetentionBytes int64
	SegmentBytes   int64
}

type Record = codec.Record

type RecordBatch struct {
	BaseOffset           int64
	Count                int32
	PartitionLeaderEpoch int32
	Attributes           int16
	BaseSequence         int32
	Records              []*Record
}

type OffsetIndexEntry struct {
	RelativeOffset uint32
	Position       uint32
}

type TimeIndexEntry struct {
	Timestamp      int64
	RelativeOffset uint32
}

type Segment struct {
	mu            sync.Mutex
	baseOffset    int64
	dir           string
	logFile       *os.File
	indexFile     *os.File
	timeIndexFile *os.File
	logMmap       []byte
	indexMmap     []byte
	timeMmap      []byte
	config        SegmentConfig
	size          int64
	indexSize     int64
	timeIndexSize int64
	closed        bool
}

type SegmentConfig struct {
	MaxLogBytes   int64
	MaxIndexBytes int64
}

func NewSegmentConfig() SegmentConfig {
	return SegmentConfig{
		MaxLogBytes:   DefaultSegmentSize,
		MaxIndexBytes: DefaultMaxIndexSize,
	}
}

type Segments struct {
	mu            sync.Mutex
	segments      []*Segment
	baseOffset    int64
	maxLogBytes   int64
	maxIndexBytes int64
	dir           string
}

type Partition struct {
	TopicName     string
	PartitionID   int32
	Segments      *Segments
	HighWatermark int64
	LogEndOffset  int64
	mu            sync.RWMutex

	// Raft integration
	IsLeader bool
	RaftNode interface{} // Will be *raft.Node, using interface to avoid import cycle
}

type Topic struct {
	Name              string
	Partitions        map[int32]*Partition
	PartitionCount    int32
	ReplicationFactor int32
	Config            TopicConfig
}

type WAL struct {
	config Config
	topics map[string]*Topic
	mu     sync.RWMutex
	closed bool
	wg     sync.WaitGroup
}

func NewWAL(config Config) (*WAL, error) {
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, err
	}

	if config.SegmentSize == 0 {
		config.SegmentSize = DefaultSegmentSize
	}
	if config.IndexInterval == 0 {
		config.IndexInterval = 4096
	}

	return &WAL{
		config: config,
		topics: make(map[string]*Topic),
	}, nil
}

func (w *WAL) CreateTopic(name string, partitions int32, config TopicConfig) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, exists := w.topics[name]; exists {
		return fmt.Errorf("topic %s already exists", name)
	}

	topicDir := filepath.Join(w.config.DataDir, "topics", name)
	if err := os.MkdirAll(topicDir, 0755); err != nil {
		return err
	}

	topic := &Topic{
		Name:              name,
		Partitions:        make(map[int32]*Partition),
		PartitionCount:    partitions,
		ReplicationFactor: 1,
		Config:            config,
	}

	for i := int32(0); i < partitions; i++ {
		partitionDir := filepath.Join(topicDir, fmt.Sprintf("partition-%d", i))
		if err := os.MkdirAll(partitionDir, 0755); err != nil {
			return err
		}

		partition := &Partition{
			TopicName:     name,
			PartitionID:   i,
			Segments:      newSegments(partitionDir, w.config.SegmentSize),
			HighWatermark: 0,
			LogEndOffset:  0,
		}
		topic.Partitions[i] = partition
	}

	w.topics[name] = topic
	return nil
}

func (w *WAL) GetTopic(name string) (*Topic, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	topic, ok := w.topics[name]
	return topic, ok
}

func (w *WAL) Topics() map[string]*Topic {
	w.mu.RLock()
	defer w.mu.RUnlock()
	result := make(map[string]*Topic)
	for k, v := range w.topics {
		result[k] = v
	}
	return result
}

func (w *WAL) GetPartition(topicName string, partitionID int32) (*Partition, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	topic, ok := w.topics[topicName]
	if !ok {
		return nil, fmt.Errorf("topic %s not found", topicName)
	}

	partition, ok := topic.Partitions[partitionID]
	if !ok {
		return nil, fmt.Errorf("partition %d not found", partitionID)
	}

	return partition, nil
}

func newSegments(dir string, maxLogBytes int64) *Segments {
	return &Segments{
		mu:            sync.Mutex{},
		segments:      make([]*Segment, 0),
		maxLogBytes:   maxLogBytes,
		maxIndexBytes: DefaultMaxIndexSize,
		dir:           dir,
	}
}

func (ss *Segments) Append(record *Record) (int64, *Segment, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	var segment *Segment
	if len(ss.segments) > 0 {
		segment = ss.segments[len(ss.segments)-1]
		if segment.size >= ss.maxLogBytes {
			segment = nil
		}
	}

	if segment == nil {
		var err error
		baseOffset := int64(0)
		if len(ss.segments) > 0 {
			baseOffset = ss.segments[len(ss.segments)-1].baseOffset + ss.segments[len(ss.segments)-1].size
		}
		segment, err = newSegment(ss.dir, baseOffset, SegmentConfig{
			MaxLogBytes:   ss.maxLogBytes,
			MaxIndexBytes: ss.maxIndexBytes,
		})
		if err != nil {
			return 0, nil, err
		}
		ss.segments = append(ss.segments, segment)
	}

	offset, err := segment.Append(record)
	return offset, segment, err
}

func newSegment(dir string, baseOffset int64, config SegmentConfig) (*Segment, error) {
	logPath := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
	indexPath := filepath.Join(dir, fmt.Sprintf("%020d.index", baseOffset))
	timeIndexPath := filepath.Join(dir, fmt.Sprintf("%020d.timeindex", baseOffset))

	logFile, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		logFile.Close()
		return nil, err
	}

	timeIndexFile, err := os.OpenFile(timeIndexPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		logFile.Close()
		indexFile.Close()
		return nil, err
	}

	segment := &Segment{
		mu:            sync.Mutex{},
		baseOffset:    baseOffset,
		dir:           dir,
		logFile:       logFile,
		indexFile:     indexFile,
		timeIndexFile: timeIndexFile,
		config:        config,
		size:          0,
		indexSize:     0,
		timeIndexSize: 0,
		closed:        false,
	}

	stat, _ := logFile.Stat()
	segment.size = stat.Size()

	stat, _ = indexFile.Stat()
	segment.indexSize = stat.Size()

	stat, _ = timeIndexFile.Stat()
	segment.timeIndexSize = stat.Size()

	if segment.size > 0 {
		segment.logMmap, _ = mapFile(logFile, segment.size)
		segment.indexMmap, _ = mapFile(indexFile, segment.indexSize)
		segment.timeMmap, _ = mapFile(timeIndexFile, segment.timeIndexSize)
	}

	return segment, nil
}

func mapFile(f *os.File, size int64) ([]byte, error) {
	if size == 0 {
		return make([]byte, 0), nil
	}
	return nil, nil
}

func (s *Segment) Append(record *Record) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, errors.New("segment closed")
	}

	encoded := codec.EncodeRecord(record)
	recordLen := len(encoded)

	totalLen := recordLen + 4
	encodedWithLen := make([]byte, totalLen)
	binary.BigEndian.PutUint32(encodedWithLen[0:4], uint32(recordLen))
	copy(encodedWithLen[4:], encoded)

	offset := s.baseOffset + s.size
	record.Offset = offset

	n, err := s.logFile.Write(encodedWithLen)
	if err != nil {
		return 0, err
	}

	if s.size%int64(s.config.MaxLogBytes/IndexEntrySize) == 0 {
		indexEntry := OffsetIndexEntry{
			RelativeOffset: uint32(offset - s.baseOffset),
			Position:       uint32(s.size),
		}
		s.writeIndexEntry(indexEntry)

		timeEntry := TimeIndexEntry{
			Timestamp:      record.Timestamp,
			RelativeOffset: uint32(offset - s.baseOffset),
		}
		s.writeTimeIndexEntry(timeEntry)
	}

	s.size += int64(n)
	return offset, nil
}

func (s *Segment) writeIndexEntry(entry OffsetIndexEntry) error {
	entryBytes := make([]byte, IndexEntrySize)
	binary.BigEndian.PutUint32(entryBytes[0:4], entry.RelativeOffset)
	binary.BigEndian.PutUint32(entryBytes[4:8], entry.Position)

	_, err := s.indexFile.Write(entryBytes)
	if err != nil {
		return err
	}
	s.indexSize += int64(IndexEntrySize)
	return nil
}

func (s *Segment) writeTimeIndexEntry(entry TimeIndexEntry) error {
	entryBytes := make([]byte, TimeIndexEntrySize)
	binary.BigEndian.PutUint64(entryBytes[0:8], uint64(entry.Timestamp))
	binary.BigEndian.PutUint32(entryBytes[8:12], entry.RelativeOffset)

	_, err := s.timeIndexFile.Write(entryBytes)
	if err != nil {
		return err
	}
	s.timeIndexSize += int64(TimeIndexEntrySize)
	return nil
}

func (s *Segment) Read(targetOffset int64) (*Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if targetOffset < s.baseOffset || targetOffset >= s.baseOffset+s.size {
		return nil, ErrOffsetOutOfRange
	}

	// Simple sequential scan for now
	pos := int64(0)
	currentOffset := s.baseOffset
	for pos < s.size {
		data := make([]byte, 4)
		_, err := s.logFile.ReadAt(data, pos)
		if err != nil {
			return nil, err
		}
		recordLen := binary.BigEndian.Uint32(data)
		pos += 4

		recordData := make([]byte, recordLen)
		_, err = s.logFile.ReadAt(recordData, pos)
		if err != nil {
			return nil, err
		}
		pos += int64(recordLen)

		// recordData starts with CRC, pass it to DecodeRecord
		record := codec.DecodeRecord(recordData)
		if record == nil {
			currentOffset++
			continue
		}

		if currentOffset == targetOffset {
			record.Offset = targetOffset
			return record, nil
		}

		currentOffset++
	}

	return nil, ErrNotFound
}

func (s *Segment) findPosition(relativeOffset uint32) uint32 {
	if s.indexSize == 0 {
		return 0
	}

	numEntries := int64(s.indexSize / IndexEntrySize)

	low := int64(0)
	high := numEntries - 1

	for low <= high {
		mid := (low + high) / 2

		offset := binary.BigEndian.Uint32(s.indexMmap[mid*IndexEntrySize:])
		pos := binary.BigEndian.Uint32(s.indexMmap[mid*IndexEntrySize+4:])

		if offset == relativeOffset {
			return pos
		} else if offset < relativeOffset {
			low = mid + 1
		} else {
			if mid == 0 {
				return ^uint32(0)
			}
			high = mid - 1
		}
	}

	if high < 0 {
		return 0
	}

	return binary.BigEndian.Uint32(s.indexMmap[high*IndexEntrySize+4:])
}

func (s *Segment) readRecordAt(position int64) (*Record, error) {
	data := make([]byte, 4)
	_, err := s.logFile.ReadAt(data, position)
	if err != nil {
		return nil, err
	}

	recordLen := binary.BigEndian.Uint32(data)
	recordData := make([]byte, recordLen)
	_, err = s.logFile.ReadAt(recordData, position+4)
	if err != nil {
		return nil, err
	}

	record := codec.DecodeRecord(recordData)
	return record, nil
}

func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true

	if s.logFile != nil {
		s.logFile.Close()
	}
	if s.indexFile != nil {
		s.indexFile.Close()
	}
	if s.timeIndexFile != nil {
		s.timeIndexFile.Close()
	}

	return nil
}

func (s *Segment) IsFull() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size >= s.config.MaxLogBytes
}

func (s *Segment) Size() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size
}

func (ss *Segments) GetSegment(offset int64) (*Segment, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	for _, seg := range ss.segments {
		if offset >= seg.baseOffset && offset < seg.baseOffset+seg.size {
			return seg, nil
		}
	}

	return nil, ErrSegmentNotFound
}

func (ss *Segments) Read(offset int64) (*Record, error) {
	seg, err := ss.GetSegment(offset)
	if err != nil {
		return nil, err
	}
	return seg.Read(offset)
}

func (p *Partition) Append(record *Record) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	offset, _, err := p.Segments.Append(record)
	if err != nil {
		return 0, err
	}

	p.LogEndOffset = offset + 1
	return offset, nil
}

func (p *Partition) Read(offset int64) (*Record, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.Segments.Read(offset)
}
