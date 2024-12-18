from confluent_kafka import Consumer, KafkaException, KafkaError
import redis
import json


# Kafka Configuration
KAFKA_TOPIC = "equity_market_data"
KAFKA_BOOTSTRAP_SERVERS = "pkc-w77k7w.centralus.azure.confluent.cloud:9092"
KAFKA_USERNAME = "BHGH5GP6T5ERDQLI"
KAFKA_PASSWORD = "D40AFZISgQBVgbFh617ZBMOSkGw0FU77GFr3HBsHLbqAkXJqvl+b/LSEMfP/wXl1"

# Redis Configuration
REDIS_HOST = "redis.finvedic.in"
REDIS_PORT = 6379
REDIS_TTL = 3600  # Set TTL for cache entries in seconds (1 hour)

# Initialize Redis client with connection pooling
redis_client = redis.StrictRedis(
    host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
)

# Kafka Consumer configuration
consumer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': KAFKA_USERNAME,
    'sasl.password': KAFKA_PASSWORD,
    'group.id': 'equity_market_group',
    'auto.offset.reset': 'earliest',  # To read messages from the beginning if no offset is stored
}

# Initialize Kafka consumer
consumer = Consumer(consumer_config)

# Subscribe to the topic
consumer.subscribe([KAFKA_TOPIC])

def consume_data():
    print("Consuming data from Kafka and storing in Redis...")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # Timeout of 1 second to check for new messages
            if msg is None:
                continue  # No message received, continue polling
            if msg.error():
                if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    print("Error: Topic not found or unavailable")
                else:
                    print(f"Consumer error: {msg.error()}")
                continue
            data = msg.value()
            ticker = data.get("ticker")
            if ticker:
                # Store the data in Redis with TTL
                redis_client.setex(ticker, REDIS_TTL, json.dumps(data))
                print(f"Stored in Redis: {ticker} -> {data}")
            else:
                print("Invalid data received:", data)
    except KafkaException as e:
        print(f"KafkaException error: {str(e)}")
    except Exception as e:
        print(f"Error while consuming data: {str(e)}")
    finally:
        consumer.close()
        print("Kafka consumer connection closed.")

if __name__ == "__main__":
    consume_data()
