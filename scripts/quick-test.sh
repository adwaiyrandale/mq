#!/bin/bash
# Quick test script for DMQ REST API
# Usage: ./scripts/quick-test.sh [API_URL]

API_URL=${1:-http://localhost:8080}
TOPIC="test-topic-$(date +%s)"

echo "=== DMQ API Quick Test ==="
echo "API URL: $API_URL"
echo "Test Topic: $TOPIC"
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

check_response() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Success${NC}"
    else
        echo -e "${RED}✗ Failed${NC}"
        exit 1
    fi
}

# 1. Health check
echo "1. Checking health..."
HEALTH=$(curl -s "$API_URL/health")
echo "   Response: $HEALTH"
check_response
echo ""

# 2. Create topic
echo "2. Creating topic '$TOPIC'..."
CREATE_RESP=$(curl -s -X POST "$API_URL/v1/topics" \
    -H "Content-Type: application/json" \
    -d "{\"topic\":\"$TOPIC\",\"partitions\":2}")
echo "   Response: $CREATE_RESP"
check_response
echo ""

# 3. List topics
echo "3. Listing topics..."
LIST_RESP=$(curl -s "$API_URL/v1/topics")
echo "   Topics: $(echo $LIST_RESP | jq -r '.topics | join(", ")')"
check_response
echo ""

# 4. Describe topic
echo "4. Describing topic '$TOPIC'..."
DESC_RESP=$(curl -s "$API_URL/v1/topics/$TOPIC")
echo "   Partitions: $(echo $DESC_RESP | jq '.partitions')"
check_response
echo ""

# 5. Produce messages
echo "5. Producing 3 messages..."
for i in {1..3}; do
    KEY=$(echo -n "key-$i" | base64)
    VALUE=$(echo -n "Hello World $i" | base64)
    PROD_RESP=$(curl -s -X POST "$API_URL/v1/topics/$TOPIC/messages" \
        -H "Content-Type: application/json" \
        -d "{\"topic\":\"$TOPIC\",\"key\":\"$KEY\",\"value\":\"$VALUE\",\"acks\":1,\"partition\":0}")
    OFFSET=$(echo $PROD_RESP | jq -r '.offset')
    echo "   Message $i → offset $OFFSET"
done
check_response
echo ""

# 6. Consume messages
echo "6. Consuming messages from partition 0..."
CONSUME_RESP=$(curl -s "$API_URL/v1/topics/$TOPIC/partitions/0/messages?offset=0")
MSG_COUNT=$(echo $CONSUME_RESP | jq '.messages | length')
echo "   Received $MSG_COUNT messages:"
echo $CONSUME_RESP | jq -r '.messages[] | "   Offset \(.offset): \(.value | @base64d)"'
check_response
echo ""

# 7. Cluster info
echo "7. Getting cluster info..."
CLUSTER_RESP=$(curl -s "$API_URL/v1/cluster")
echo "   Broker ID: $(echo $CLUSTER_RESP | jq '.broker_id')"
echo "   Cluster ID: $(echo $CLUSTER_RESP | jq -r '.cluster_id')"
echo "   Brokers: $(echo $CLUSTER_RESP | jq '.brokers | length')"
check_response
echo ""

# 8. Consumer group test
echo "8. Testing consumer group..."

# Join group
JOIN_RESP=$(curl -s -X POST "$API_URL/v1/groups/test-group/join" \
    -H "Content-Type: application/json" \
    -d '{"group_id":"test-group","member_id":"","topics":["'$TOPIC'"]}')
MEMBER_ID=$(echo $JOIN_RESP | jq -r '.member_id')
GEN_ID=$(echo $JOIN_RESP | jq -r '.generation_id')
echo "   Joined as member: $MEMBER_ID"

# Sync group
SYNC_RESP=$(curl -s -X POST "$API_URL/v1/groups/test-group/sync" \
    -H "Content-Type: application/json" \
    -d "{\"group_id\":\"test-group\",\"member_id\":\"$MEMBER_ID\",\"generation_id\":$GEN_ID}")
echo "   Assignment: $(echo $SYNC_RESP | jq -r '.assignment | keys | join(", ")')"

# Commit offset
COMMIT_RESP=$(curl -s -X POST "$API_URL/v1/groups/test-group/offsets" \
    -H "Content-Type: application/json" \
    -d "{\"group_id\":\"test-group\",\"topic\":\"$TOPIC\",\"partition\":0,\"offset\":3}")
echo "   Offset committed: $(echo $COMMIT_RESP | jq '.success')"

# Leave group
LEAVE_RESP=$(curl -s -X POST "$API_URL/v1/groups/test-group/leave" \
    -H "Content-Type: application/json" \
    -d "{\"group_id\":\"test-group\",\"member_id\":\"$MEMBER_ID\"}")
echo "   Left group: $(echo $LEAVE_RESP | jq '.success')"
check_response
echo ""

echo -e "${GREEN}=== All tests passed! ===${NC}"
echo ""
echo "Swagger UI: $API_URL/swagger/"
