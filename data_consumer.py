import json
import logging
import redis
from confluent_kafka import Consumer, KafkaException, KafkaError

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("DataConsumer")

# Redis configuration
REDIS_HOST = "localhost"  # Localhost connection
REDIS_PORT = 6379         # Default Redis port
REDIS_DB = 0              # Use default Redis DB

# Initialize Redis client
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

# Kafka configuration
KAFKA_TOPIC = "stock_orders"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"  # Localhost Kafka Broker
KAFKA_GROUP_ID = "data_consumer_group"

consumer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': KAFKA_GROUP_ID,
    'auto.offset.reset': 'earliest'  # Start consuming from the earliest messages
}

def store_in_redis(data):
    """
    Store the consumed data into Redis.
    Args:
        data (dict): Market data received from Kafka Consumer.
    """
    try:
        # Use ticker as the key and store the data as JSON
        ticker = data['ticker']
        redis_client.set(ticker, json.dumps(data))
        logger.info(f"Data for {ticker} stored in Redis successfully.")
    except Exception as e:
        logger.error(f"Failed to store data in Redis: {e}")

def consume_data():
    """
    Consumes data from the Kafka topic and stores it in Redis.
    """
    consumer = Consumer(consumer_config)
    try:
        consumer.subscribe([KAFKA_TOPIC])
        logger.info(f"Subscribed to Kafka topic: {KAFKA_TOPIC}")
        
        while True:
            msg = consumer.poll(1.0)  # Poll messages with a 1-second timeout
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.warning("Reached end of partition")
                elif msg.error():
                    raise KafkaException(msg.error())
                continue

            # Decode the message and parse JSON
            data = json.loads(msg.value().decode('utf-8'))
            logger.info(f"Received message: {data}")
            
            # Store the data in Redis
            store_in_redis(data)

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")
    finally:
        consumer.close()
        logger.info("Consumer closed.")

if __name__ == "__main__":
    consume_data()