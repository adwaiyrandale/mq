# DMQ Examples and Use Cases

This guide provides step-by-step examples for using the DMQ REST API.

## Table of Contents

1. [Basic Topic Operations](#basic-topic-operations)
2. [Producing Messages](#producing-messages)
3. [Consuming Messages](#consuming-messages)
4. [Consumer Groups](#consumer-groups)
5. [Complete Workflow Examples](#complete-workflow-examples)

---

## Prerequisites

Start the broker and gateway:

```bash
# Terminal 1: Start broker
go run ./cmd/broker -id=broker-1 -port=9001 -data=./data

# Terminal 2: Start gateway
go run ./cmd/gateway
# Or on custom port:
GATEWAY_ADDR=:9090 go run ./cmd/gateway
```

Base URL: `http://localhost:8080` (or your custom port)

---

## Basic Topic Operations

### 1. Create a Topic

Create a topic with 3 partitions:

```bash
curl -X POST http://localhost:8080/v1/topics \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "orders",
    "partitions": 3,
    "replication_factor": 1
  }'
```

**Response:**
```json
{
  "success": true
}
```

**Via Swagger UI:**
1. Go to `POST /v1/topics`
2. Click "Try it out"
3. Enter request body:
   ```json
   {
     "topic": "orders",
     "partitions": 3,
     "replication_factor": 1
   }
   ```
4. Click "Execute"

---

### 2. List All Topics

```bash
curl http://localhost:8080/v1/topics
```

**Response:**
```json
{
  "topics": ["orders", "__consumer_offsets"]
}
```

---

### 3. Describe a Topic

```bash
curl http://localhost:8080/v1/topics/orders
```

**Response:**
```json
{
  "topic": "orders",
  "partitions": 3,
  "partition_info": [
    {
      "partition_id": 0,
      "high_water_mark": 0,
      "log_end_offset": 0,
      "leader": 1,
      "replicas": [1],
      "isr": [1]
    },
    {
      "partition_id": 1,
      "high_water_mark": 0,
      "log_end_offset": 0,
      "leader": 1,
      "replicas": [1],
      "isr": [1]
    },
    {
      "partition_id": 2,
      "high_water_mark": 0,
      "log_end_offset": 0,
      "leader": 1,
      "replicas": [1],
      "isr": [1]
    }
  ]
}
```

---

## Producing Messages

### 4. Produce a Message (Base64 Encoded)

Messages must be base64 encoded:

```bash
# Key: "order-001", Value: "{\"id\": 1, \"total\": 100.00}"
curl -X POST http://localhost:8080/v1/topics/orders/messages \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "orders",
    "key": "b3JkZXItMDAx",
    "value": "eyJpZCI6IDEsICJ0b3RhbCI6IDEwMC4wMH0=",
    "acks": 1,
    "partition": 0
  }'
```

**Note:** Use `echo -n "order-001" | base64` to encode strings.

**Response:**
```json
{
  "offset": 0,
  "partition": 0,
  "timestamp": 1709700000000
}
```

---

### 5. Produce Multiple Messages

```bash
# Produce 5 messages in a loop
for i in {1..5}; do
  KEY=$(echo -n "key-$i" | base64)
  VALUE=$(echo -n "message-$i-content" | base64)
  curl -X POST http://localhost:8080/v1/topics/orders/messages \
    -H "Content-Type: application/json" \
    -d "{\"topic\":\"orders\",\"key\":\"$KEY\",\"value\":\"$VALUE\",\"acks\":1,\"partition\":0}"
done
```

---

## Consuming Messages

### 6. Consume from Beginning (Offset 0)

```bash
curl "http://localhost:8080/v1/topics/orders/partitions/0/messages?offset=0&max_bytes=1048576"
```

**Response:**
```json
{
  "messages": [
    {
      "key": "b3JkZXItMDAx",
      "value": "eyJpZCI6IDEsICJ0b3RhbCI6IDEwMC4wMH0=",
      "headers": {},
      "timestamp": 1709700000000,
      "offset": 0,
      "partition": 0,
      "topic": "orders"
    }
  ],
  "high_watermark": 1
}
```

**Decode the message:**
```bash
echo "eyJpZCI6IDEsICJ0b3RhbCI6IDEwMC4wMH0=" | base64 -d
# Output: {"id": 1, "total": 100.00}
```

---

### 7. Consume from Specific Offset

```bash
# Start from offset 5
curl "http://localhost:8080/v1/topics/orders/partitions/0/messages?offset=5"

# Get only 2 messages
curl "http://localhost:8080/v1/topics/orders/partitions/0/messages?offset=0&max_bytes=1024"
```

---

## Consumer Groups

### 8. Join a Consumer Group

```bash
curl -X POST http://localhost:8080/v1/groups/my-consumer-group/join \
  -H "Content-Type: application/json" \
  -d '{
    "group_id": "my-consumer-group",
    "member_id": "",
    "topics": ["orders"]
  }'
```

**Response:**
```json
{
  "member_id": "member-abc123",
  "generation_id": 1,
  "leader_id": "member-abc123",
  "members": ["member-abc123"]
}
```

**Save the member_id for later use:**
```bash
MEMBER_ID="member-abc123"
```

---

### 9. Sync Group (Get Partition Assignment)

After joining, sync to get partition assignments:

```bash
curl -X POST http://localhost:8080/v1/groups/my-consumer-group/sync \
  -H "Content-Type: application/json" \
  -d '{
    "group_id": "my-consumer-group",
    "member_id": "member-abc123",
    "generation_id": 1
  }'
```

**Response:**
```json
{
  "success": true,
  "assignment": {
    "orders": {
      "partitions": [0, 1, 2]
    }
  }
}
```

---

### 10. Check Committed Offset

Consumer offsets are tracked in the `__consumer_offsets` topic. To check current position:

```bash
# Consume from the internal offsets topic
curl "http://localhost:8080/v1/topics/__consumer_offsets/partitions/0/messages?offset=0"
```

**Or use the consumer flow:**

After consuming messages, commit the offset:

```bash
curl -X POST http://localhost:8080/v1/groups/my-consumer-group/offsets \
  -H "Content-Type: application/json" \
  -d '{
    "group_id": "my-consumer-group",
    "topic": "orders",
    "partition": 0,
    "offset": 10
  }'
```

**Response:**
```json
{
  "success": true
}
```

---

### 11. Send Heartbeat

Keep the consumer alive:

```bash
curl -X POST http://localhost:8080/v1/groups/my-consumer-group/heartbeat \
  -H "Content-Type: application/json" \
  -d '{
    "group_id": "my-consumer-group",
    "member_id": "member-abc123",
    "generation_id": 1
  }'
```

---

### 12. Leave Group

```bash
curl -X POST http://localhost:8080/v1/groups/my-consumer-group/leave \
  -H "Content-Type: application/json" \
  -d '{
    "group_id": "my-consumer-group",
    "member_id": "member-abc123"
  }'
```

---

## Complete Workflow Examples

### Example 1: Simple Producer-Consumer

```bash
#!/bin/bash

API_URL="http://localhost:8080"
TOPIC="test-topic"

# 1. Create topic
echo "Creating topic..."
curl -s -X POST "$API_URL/v1/topics" \
  -H "Content-Type: application/json" \
  -d "{\"topic\":\"$TOPIC\",\"partitions\":1}"

# 2. Produce messages
echo -e "\nProducing messages..."
for i in {1..3}; do
  KEY=$(echo -n "key-$i" | base64)
  VALUE=$(echo -n "Hello World $i" | base64)
  curl -s -X POST "$API_URL/v1/topics/$TOPIC/messages" \
    -H "Content-Type: application/json" \
    -d "{\"topic\":\"$TOPIC\",\"key\":\"$KEY\",\"value\":\"$VALUE\",\"acks\":1,\"partition\":0}"
  echo "Produced message $i"
done

# 3. Consume messages
echo -e "\nConsuming messages..."
RESPONSE=$(curl -s "$API_URL/v1/topics/$TOPIC/partitions/0/messages?offset=0")
echo "$RESPONSE" | jq -r '.messages[] | "Offset: \(.offset), Value: \(.value)"'

# 4. Decode values
echo -e "\nDecoded values:"
echo "$RESPONSE" | jq -r '.messages[].value' | while read -r val; do
  echo "$val" | base64 -d
  echo
done
```

---

### Example 2: Consumer Group with Multiple Members

**Terminal 1 - Consumer 1:**
```bash
# Join group
RESP=$(curl -s -X POST http://localhost:8080/v1/groups/multi-consumer-group/join \
  -H "Content-Type: application/json" \
  -d '{"group_id":"multi-consumer-group","member_id":"","topics":["orders"]}')

MEMBER1=$(echo $RESP | jq -r '.member_id')
GEN_ID=$(echo $RESP | jq -r '.generation_id')
echo "Member 1 ID: $MEMBER1"

# Sync to get assignment
curl -s -X POST http://localhost:8080/v1/groups/multi-consumer-group/sync \
  -H "Content-Type: application/json" \
  -d "{\"group_id\":\"multi-consumer-group\",\"member_id\":\"$MEMBER1\",\"generation_id\":$GEN_ID}"

# Send periodic heartbeats
while true; do
  curl -s -X POST http://localhost:8080/v1/groups/multi-consumer-group/heartbeat \
    -H "Content-Type: application/json" \
    -d "{\"group_id\":\"multi-consumer-group\",\"member_id\":\"$MEMBER1\",\"generation_id\":$GEN_ID}"
  sleep 3
done
```

**Terminal 2 - Consumer 2:**
```bash
# Join same group
RESP=$(curl -s -X POST http://localhost:8080/v1/groups/multi-consumer-group/join \
  -H "Content-Type: application/json" \
  -d '{"group_id":"multi-consumer-group","member_id":"","topics":["orders"]}')

MEMBER2=$(echo $RESP | jq -r '.member_id')
GEN_ID=$(echo $RESP | jq -r '.generation_id')
echo "Member 2 ID: $MEMBER2"

# Sync - partitions will be rebalanced
curl -s -X POST http://localhost:8080/v1/groups/multi-consumer-group/sync \
  -H "Content-Type: application/json" \
  -d "{\"group_id\":\"multi-consumer-group\",\"member_id\":\"$MEMBER2\",\"generation_id\":$GEN_ID}"
```

---

### Example 3: Real-time Event Stream

Simulate an e-commerce order system:

```bash
#!/bin/bash

API_URL="http://localhost:8080"
TOPIC="ecommerce-orders"

# Create topic
curl -s -X POST "$API_URL/v1/topics" \
  -H "Content-Type: application/json" \
  -d "{\"topic\":\"$TOPIC\",\"partitions\":3}"

echo "Producing orders..."

# Produce orders continuously
for i in {1..100}; do
  ORDER_ID="ORD-$(date +%s)-$i"
  KEY=$(echo -n "$ORDER_ID" | base64)
  
  # Create order JSON
  ORDER_JSON=$(cat <<EOF
{
  "order_id": "$ORDER_ID",
  "customer_id": "CUST-$((RANDOM % 1000))",
  "items": [
    {"product": "Widget", "qty": $((RANDOM % 5 + 1)), "price": 29.99}
  ],
  "total": $((RANDOM % 200 + 50)).00,
  "status": "PLACED",
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF
)
  VALUE=$(echo -n "$ORDER_JSON" | base64)
  
  # Determine partition by order_id hash (simplified)
  PARTITION=$((i % 3))
  
  curl -s -X POST "$API_URL/v1/topics/$TOPIC/messages" \
    -H "Content-Type: application/json" \
    -d "{\"topic\":\"$TOPIC\",\"key\":\"$KEY\",\"value\":\"$VALUE\",\"acks\":1,\"partition\":$PARTITION}"
  
  echo "Produced order $ORDER_ID to partition $PARTITION"
  sleep 0.5
done
```

**Consume from all partitions:**

```bash
#!/bin/bash

API_URL="http://localhost:8080"
TOPIC="ecommerce-orders"

echo "Consuming from all partitions..."

for partition in {0..2}; do
  echo -e "\n=== Partition $partition ==="
  
  RESPONSE=$(curl -s "$API_URL/v1/topics/$TOPIC/partitions/$partition/messages?offset=0")
  
  # Decode and display messages
  echo "$RESPONSE" | jq -r '.messages[] | 
    "Offset: \(.offset)\nKey: \(.key | @base64d)\nValue: \(.value | @base64d)\n---"
  '
done
```

---

## Testing with Swagger UI

### Step-by-Step Test Flow

1. **Open Swagger UI:** http://localhost:8080/swagger/

2. **Create Topic:**
   - Find `POST /v1/topics`
   - Click "Try it out"
   - Enter:
     ```json
     {
       "topic": "demo-topic",
       "partitions": 2,
       "replication_factor": 1
     }
     ```
   - Execute

3. **Produce Message:**
   - Find `POST /v1/topics/{topic}/messages`
   - topic: `demo-topic`
   - Body:
     ```json
     {
       "topic": "demo-topic",
       "key": "ZGVtby1rZXk=",
       "value": "SGVsbG8gZnJvbSBEUUE=",
       "acks": 1,
       "partition": 0
     }
     ```
   - Execute

4. **Consume Messages:**
   - Find `GET /v1/topics/{topic}/partitions/{partition}/messages`
   - topic: `demo-topic`
   - partition: `0`
   - offset: `0`
   - Execute

5. **View Response:**
   - Check "Responses" section
   - Look for 200 status
   - View message data in response body

---

## Troubleshooting

### Issue: Cannot connect to swagger

```bash
# Check if services are running
curl http://localhost:8080/health

# Check ports
lsof -i :9001  # Broker
lsof -i :8080  # Gateway

# Restart services
pkill -f "cmd/broker"
pkill -f "cmd/gateway"
go run ./cmd/broker -id=broker-1 -port=9001 -data=./data &
go run ./cmd/gateway &
```

### Issue: Base64 encoding

```bash
# Encode
echo -n "my-value" | base64
# Output: bXktdmFsdWU=

# Decode
echo "bXktdmFsdWU=" | base64 -d
# Output: my-value
```

### Issue: Consumer not receiving messages

- Check topic exists: `GET /v1/topics/{topic}`
- Verify partition number is correct
- Check offset is valid (use 0 to start from beginning)
- Ensure messages were produced with correct partition

---

## Next Steps

- Try the [benchmark tool](../cmd/benchmark) for performance testing
- Explore [consumer groups](EXAMPLES.md#consumer-groups) for parallel processing
- Check the [internal storage](../internal/storage) for persistence details
