"""
Kafka Configuration Module
"""

# Kafka Broker Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'

# Topic Configuration
ORDER_TOPIC = 'orders'
DLQ_TOPIC = 'orders-dlq'
RETRY_TOPIC = 'orders-retry'

# Producer Configuration
PRODUCER_CONFIG = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'schema.registry.url': SCHEMA_REGISTRY_URL,
}

# Consumer Configuration
CONSUMER_CONFIG = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'schema.registry.url': SCHEMA_REGISTRY_URL,
    'group.id': 'order-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
}

# Retry Configuration
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2