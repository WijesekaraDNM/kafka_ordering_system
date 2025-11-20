"""
Kafka Order Consumer with:
- Avro Deserialization
- Real-time Price Aggregation (Running Average)
- Retry Logic for Temporary Failures
- Dead Letter Queue (DLQ) for Permanent Failures
"""

import time
import sys
import os
from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.kafka_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    SCHEMA_REGISTRY_URL,
    ORDER_TOPIC,
    DLQ_TOPIC,
    RETRY_TOPIC,
    MAX_RETRIES,
    RETRY_DELAY_SECONDS
)


class OrderConsumer:
    def __init__(self):
        # Load Avro schema
        with open('schemas/order.avsc', 'r') as schema_file:
            schema_str = schema_file.read()
        
        # Initialize Schema Registry Client
        schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        
        # Create Avro Deserializer
        self.avro_deserializer = AvroDeserializer(
            schema_registry_client,
            schema_str,
            self.dict_to_order
        )
        
        # Create Avro Serializer for DLQ
        self.avro_serializer = AvroSerializer(
            schema_registry_client,
            schema_str,
            lambda order, ctx: order
        )
        
        # Initialize Consumer
        consumer_conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'order-consumer-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        self.consumer = Consumer(consumer_conf)
        
        # Initialize Producer for DLQ and Retry
        producer_conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id': 'order-consumer-dlq-producer'
        }
        self.dlq_producer = Producer(producer_conf)
        
        # Running statistics
        self.total_orders = 0
        self.total_price = 0.0
        self.running_average = 0.0
        self.retry_counts = {}
        
    @staticmethod
    def dict_to_order(obj, ctx):
        """Convert dictionary to order object"""
        if obj is None:
            return None
        return {
            'orderId': obj['orderId'],
            'product': obj['product'],
            'price': obj['price']
        }
    
    def process_order(self, order):
        """
        Process an order - simulates business logic
        Randomly fails 20% of the time to demonstrate retry/DLQ
        """
        import random
        
        # Simulate processing failure (20% chance)
        if random.random() < 0.2:
            raise Exception("Simulated temporary processing failure")
        
        # Successfully process order
        self.total_orders += 1
        self.total_price += order['price']
        self.running_average = self.total_price / self.total_orders
        
        print(f"Processed Order {order['orderId']}: {order['product']} - ${order['price']:.2f}")
        print(f"Running Average Price: ${self.running_average:.2f} (from {self.total_orders} orders)")
        
    def send_to_dlq(self, order, reason):
        """Send failed message to Dead Letter Queue"""
        try:
            dlq_message = {
                'orderId': order['orderId'],
                'product': order['product'],
                'price': order['price']
            }
            
            self.dlq_producer.produce(
                topic=DLQ_TOPIC,
                key=str(order['orderId']),
                value=self.avro_serializer(
                    dlq_message,
                    SerializationContext(DLQ_TOPIC, MessageField.VALUE)
                ),
                headers={'failure_reason': reason.encode('utf-8')}
            )
            self.dlq_producer.flush()
            print(f"💀 Sent to DLQ - Order {order['orderId']}: {reason}")
        except Exception as e:
            print(f"Failed to send to DLQ: {e}")
    
    def handle_message(self, msg):
        """Handle a single message with retry logic"""
        try:
            # Deserialize message
            order = self.avro_deserializer(
                msg.value(),
                SerializationContext(msg.topic(), MessageField.VALUE)
            )
            
            if order is None:
                print("Received empty order, skipping...")
                return True
            
            order_id = order['orderId']
            
            # Initialize retry count for this order
            if order_id not in self.retry_counts:
                self.retry_counts[order_id] = 0
            
            # Try to process the order
            try:
                self.process_order(order)
                
                # Success - reset retry count
                if order_id in self.retry_counts:
                    del self.retry_counts[order_id]
                
                return True
                
            except Exception as e:
                # Processing failed
                self.retry_counts[order_id] += 1
                
                if self.retry_counts[order_id] < MAX_RETRIES:
                    # Temporary failure - retry
                    print(f"Order {order_id} failed (attempt {self.retry_counts[order_id]}/{MAX_RETRIES}): {e}")
                    print(f"Retrying in {RETRY_DELAY_SECONDS} seconds...")
                    time.sleep(RETRY_DELAY_SECONDS)
                    return False  # Don't commit, will retry
                else:
                    # Permanent failure - send to DLQ
                    print(f"Order {order_id} permanently failed after {MAX_RETRIES} attempts")
                    self.send_to_dlq(order, f"Failed after {MAX_RETRIES} retries: {str(e)}")
                    del self.retry_counts[order_id]
                    return True  # Commit to move on
                    
        except Exception as e:
            print(f"Error deserializing message: {e}")
            return True  # Commit to skip bad message
    
    def consume_orders(self):
        """Main consume loop"""
        self.consumer.subscribe([ORDER_TOPIC])
        
        print(f"\nSubscribed to topic: {ORDER_TOPIC}")
        print(f"Real-time aggregation enabled")
        print(f"Retry logic: {MAX_RETRIES} attempts")
        print(f"DLQ topic: {DLQ_TOPIC}")
        print("\nWaiting for messages...\n")
        
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print(f"📭 Reached end of partition {msg.partition()}")
                    else:
                        print(f"Consumer error: {msg.error()}")
                    continue
                
                # Process message with retry logic
                success = self.handle_message(msg)
                
                # Commit offset only if processing succeeded or permanently failed
                if success:
                    self.consumer.commit(msg)
                
        except KeyboardInterrupt:
            print("\n\nConsumer stopped by user")
        finally:
            self.consumer.close()
            self.dlq_producer.flush()
            print(f"\nFinal Statistics:")
            print(f"   Total Orders Processed: {self.total_orders}")
            print(f"   Total Revenue: ${self.total_price:.2f}")
            print(f"   Average Order Price: ${self.running_average:.2f}")


def main():
    """Main function"""
    print("=" * 60)
    print("KAFKA ORDER CONSUMER")
    print("=" * 60)
    
    consumer = OrderConsumer()
    consumer.consume_orders()


if __name__ == "__main__":
    main()