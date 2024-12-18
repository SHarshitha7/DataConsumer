from app import initialize_app

app = initialize_app()
if __name__=='__main__':
    app.run(debug=True)

'''from kafka import KafkaConsumer
import redis
import json
import config

# Connect to Kafka
consumer = KafkaConsumer(
    'equity_market_data',
    bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
    security_protocol=config.KAFKA_SECURITY_PROTOCOL,
    sasl_mechanism=config.KAFKA_SASL_MECHANISM,
    sasl_plain_username=config.KAFKA_SASL_USERNAME,
    sasl_plain_password=config.KAFKA_SASL_PASSWORD,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Connect to Redis
r = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=0)

for message in consumer:
    data = message.value
    print(f"Consumed data: {data}")
    r.set(data['ticker'], json.dumps(data))  # Ensure data is serialized as JSON'''