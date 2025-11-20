#!/bin/bash

echo "Creating Kafka Topics..."

# Create main orders topic
docker exec kafka kafka-topics --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server kafka:9092 \
  --if-not-exists

echo "Created 'orders' topic"

# Create DLQ topic
docker exec kafka kafka-topics --create \
  --topic orders-dlq \
  --partitions 1 \
  --replication-factor 1 \
  --bootstrap-server kafka:9092 \
  --if-not-exists

echo "Created 'orders-dlq' topic"

# Create retry topic
docker exec kafka kafka-topics --create \
  --topic orders-retry \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server kafka:9092 \
  --if-not-exists

echo "Created 'orders-retry' topic"

# List all topics
echo ""
echo "All topics:"
docker exec kafka kafka-topics --list --bootstrap-server kafka:9092