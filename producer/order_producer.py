"""
Kafka Order Producer with Avro Serialization
"""

import time
import random
import json
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.kafka_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    SCHEMA_REGISTRY_URL,
    ORDER_TOPIC
)


class OrderProducer:
    def __init__(self):
        # Load Avro schema
        with open('schemas/order.avsc', 'r') as schema_file:
            schema_str = schema_file.read()
        
        # Initialize Schema Registry Client
        schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        
        # Create Avro Serializer
        self.avro_serializer = AvroSerializer(
            schema_registry_client,
            schema_str,
            self.order_to_dict
        )
        
        # Initialize Producer
        producer_conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id': 'order-producer'
        }
        self.producer = Producer(producer_conf)
        
        # Product catalog
        self.products = [
            'Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones',
            'Webcam', 'USB Cable', 'Phone', 'Tablet', 'Charger'
        ]
    
    @staticmethod
    def order_to_dict(order, ctx):
        """Convert order object to dictionary for serialization"""
        return {
            'orderId': order['orderId'],
            'product': order['product'],
            'price': order['price']
        }
    
    def delivery_report(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
    
    def create_order(self, order_id):
        """Generate a random order"""
        return {
            'orderId': str(order_id),
            'product': random.choice(self.products),
            'price': round(random.uniform(10.0, 1000.0), 2)
        }
    
    def produce_orders(self, num_orders=20):
        """Produce specified number of orders"""
        print(f"\nStarting to produce {num_orders} orders...\n")
        
        for i in range(1, num_orders + 1):
            try:
                order = self.create_order(1000 + i)
                
                # Serialize and produce message
                self.producer.produce(
                    topic=ORDER_TOPIC,
                    key=str(order['orderId']),
                    value=self.avro_serializer(
                        order,
                        SerializationContext(ORDER_TOPIC, MessageField.VALUE)
                    ),
                    on_delivery=self.delivery_report
                )
                
                print(f"Produced Order {order['orderId']}: {order['product']} - ${order['price']}")
                
                # Poll for delivery reports
                self.producer.poll(0)
                
                # Small delay between messages
                time.sleep(0.5)
                
            except Exception as e:
                print(f"Error producing order {i}: {e}")
        
        # Wait for all messages to be delivered
        print("\nFlushing remaining messages...")
        self.producer.flush()
        print("All messages sent!\n")


def main():
    """Main function"""
    print("=" * 60)
    print("KAFKA ORDER PRODUCER")
    print("=" * 60)
    
    producer = OrderProducer()
    
    # Produce 20 orders as per assignment requirement
    producer.produce_orders(num_orders=20)
    
    print("Producer completed successfully!")


if __name__ == "__main__":
    main()