# Kafka Order Processing System

A complete Kafka-based order processing system with Avro serialization, real-time aggregation, retry logic, and Dead Letter Queue (DLQ) implementation.

## Features

- **Avro Serialization**: All messages use Avro schema for efficient serialization
- **Real-time Aggregation**: Running average calculation of order prices
- **Retry Logic**: Automatic retry for temporary failures (3 attempts)
- **Dead Letter Queue**: Failed messages sent to DLQ after max retries
- **Consumer Groups**: Scalable consumption with partition assignment

## Project Structure

kafka-order-system/
├── docker-compose.yml          # Kafka cluster configuration
├── requirements.txt            # Python dependencies
├── schemas/
│   └── order.avsc             # Avro schema definition
├── producer/
│   └── order_producer.py      # Order message producer
├── consumer/
│   └── order_consumer.py      # Order message consumer
├── config/
│   └── kafka_config.py        # Kafka configuration
├── create_topics.sh           # Topic creation script
└── README.md                  # This file

## Prerequisites

- Docker & Docker Compose
- Python 3.8+
- VSCode (recommended)

## Installation Steps

1. **Clone/Create Project Directory**
bashmkdir kafka-order-system
cd kafka-order-system
2. **Create All Project Files**
Create the directory structure and copy all provided files into their respective locations.
3. **Start Kafka Cluster**
bashdocker-compose up -d
**Then verify**
bashdocker ps
**on the terminal** zookeeper, kafka, and schema-registry running.
4. **Install Python Dependencies**
bashpip install -r requirements.txt
5. **Create Kafka Topics**
*On Linux/Mac*:
- bashchmod +x create_topics.sh
./create_topics.sh

- *On Windows (PowerShell)*:
powershelldocker exec kafka kafka-topics --create --topic orders --partitions 3 --replication-factor 1 --bootstrap-server kafka:9092 --if-not-exists
docker exec kafka kafka-topics --create --topic orders-dlq --partitions 1 --replication-factor 1 --bootstrap-server kafka:9092 --if-not-exists

6. **Running the System**
Start Consumer (Terminal 1)
bashpython consumer/order_consumer.py

**Expected output**:
============================================================
KAFKA ORDER CONSUMER
============================================================

Subscribed to topic: orders
Real-time aggregation enabled
Retry logic: 3 attempts
DLQ topic: orders-dlq

Waiting for messages...
Start Producer (Terminal 2)
bashpython producer/order_producer.py
The producer will send 20 orders. You'll see output like:
Produced Order 1001: Laptop - $859.32
Message delivered to orders [0] at offset 0
Monitor Consumer Output
The consumer will show:

Successfully processed orders!
Running average price updates
Retry attempts for failed orders
Messages sent to DLQ after max retries

## Testing the System

1. **Verify Topic Creation**
bashdocker exec kafka kafka-topics --list --bootstrap-server kafka:9092
2. **Check Messages in DLQ**
bashdocker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic orders-dlq \
  --from-beginning \
  --max-messages 5
3. **Monitor Consumer Group**
bashdocker exec kafka kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --group order-consumer-group \
  --describe
4. **Test with Multiple Consumers**
Open a third terminal and start another consumer:
bashpython consumer/order_consumer.py
Both consumers will share partitions and process messages in parallel.


**Producer Flow**

Generates random orders (orderId, product, price)
Serializes using Avro schema
Sends to orders topic
Partitioned by orderId for ordering guarantees

**Consumer Flow**

Subscribes to orders topic
Deserializes messages using Avro
Attempts to process order
On Success: Updates running average, commits offset
On Temporary Failure: Retries up to 3 times with 2s delay
On Permanent Failure: Sends to DLQ, commits offset

**Real-time Aggregation**

Maintains running total of prices
Calculates average: total_price / total_orders
Updates displayed after each successful order

## Troubleshooting

**Kafka not accessible**
bashdocker-compose down -v
docker-compose up -d
**Schema Registry errors**
- Check if schema registry is running:
bashcurl http://localhost:8081/subjects
**Consumer not receiving messages**
- Reset consumer group:
bashdocker exec kafka kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --group order-consumer-group \
  --reset-offsets \
  --to-earliest \
  --topic orders \
  --execute
**Cleanup**
- Stop and remove all containers:
bashdocker-compose down -v

