package codec

import (
	"encoding/binary"
	"hash/crc32"
)

type Record struct {
	Offset    int64
	Timestamp int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
}

func EncodeRecord(record *Record) []byte {
	var msg []byte

	msg = append(msg, 0)

	ts := make([]byte, 8)
	binary.BigEndian.PutUint64(ts, uint64(record.Timestamp))
	msg = append(msg, ts...)

	if record.Key == nil {
		msg = append(msg, 0xff, 0xff, 0xff, 0xff)
	} else {
		keyLen := make([]byte, 4)
		binary.BigEndian.PutUint32(keyLen, uint32(len(record.Key)))
		msg = append(msg, keyLen...)
		msg = append(msg, record.Key...)
	}

	if record.Value == nil {
		msg = append(msg, 0xff, 0xff, 0xff, 0xff)
	} else {
		valLen := make([]byte, 4)
		binary.BigEndian.PutUint32(valLen, uint32(len(record.Value)))
		msg = append(msg, valLen...)
		msg = append(msg, record.Value...)
	}

	hdrCount := make([]byte, 4)
	binary.BigEndian.PutUint32(hdrCount, uint32(len(record.Headers)))
	msg = append(msg, hdrCount...)

	for k, v := range record.Headers {
		kLen := make([]byte, 2)
		binary.BigEndian.PutUint16(kLen, uint16(len(k)))
		msg = append(msg, kLen...)
		msg = append(msg, k...)

		vLen := make([]byte, 2)
		binary.BigEndian.PutUint16(vLen, uint16(len(v)))
		msg = append(msg, vLen...)
		msg = append(msg, v...)
	}

	crc := crc32.ChecksumIEEE(msg)

	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(msg)))

	result := make([]byte, 0, 4+4+len(msg))
	crcBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(crcBytes, crc)
	result = append(result, crcBytes...)
	result = append(result, length...)
	result = append(result, msg...)

	return result
}

func DecodeRecord(data []byte) *Record {
	offset := 0

	if len(data) < 9 {
		return nil
	}

	offset += 4

	length := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	if int(length) != len(data)-8 {
		return nil
	}

	offset++

	timestamp := int64(binary.BigEndian.Uint64(data[offset : offset+8]))
	offset += 8

	keyLen := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	var key []byte
	if keyLen >= 0 && uint32(keyLen) != ^uint32(0) {
		key = data[offset : offset+int(keyLen)]
		offset += int(keyLen)
	}

	valueLen := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	var value []byte
	if valueLen >= 0 && uint32(valueLen) != ^uint32(0) {
		value = data[offset : offset+int(valueLen)]
		offset += int(valueLen)
	}

	headerCount := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	headers := make(map[string]string, headerCount)
	for i := uint32(0); i < headerCount; i++ {
		hdrKeyLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2
		headerKey := string(data[offset : offset+hdrKeyLen])
		offset += hdrKeyLen

		hdrValLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2
		headerValue := string(data[offset : offset+hdrValLen])
		offset += hdrValLen

		headers[headerKey] = headerValue
	}

	return &Record{
		Offset:    0,
		Timestamp: timestamp,
		Key:       key,
		Value:     value,
		Headers:   headers,
	}
}
