#!/bin/bash

# Start a 3-node cluster for testing

set -e

echo "=== Starting MQ Cluster ==="

# Create data directories
mkdir -p data/node1 data/node2 data/node3

# Build the broker
echo "Building broker..."
go build -o bin/broker ./cmd/broker

# Start broker 1
echo "Starting broker 1 on port 9001..."
./bin/broker -id=1 -port=9001 -data=./data/node1 &
PID1=$!
sleep 2

# Start broker 2
echo "Starting broker 2 on port 9002..."
./bin/broker -id=2 -port=9002 -data=./data/node2 -peers="1:localhost:9001" &
PID2=$!
sleep 2

# Start broker 3
echo "Starting broker 3 on port 9003..."
./bin/broker -id=3 -port=9003 -data=./data/node3 -peers="1:localhost:9001,2:localhost:9002" &
PID3=$!

echo ""
echo "Cluster started!"
echo "  Broker 1: localhost:9001 (PID: $PID1)"
echo "  Broker 2: localhost:9002 (PID: $PID2)"
echo "  Broker 3: localhost:9003 (PID: $PID3)"
echo ""
echo "Press Ctrl+C to stop all brokers"

# Wait for interrupt
trap "echo 'Stopping brokers...'; kill $PID1 $PID2 $PID3 2>/dev/null; exit 0" INT
wait
