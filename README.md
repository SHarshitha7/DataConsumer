## Data Consumer
## Overview
This project consumes market data from a Kafka topic equity_market_data, processes it, and stores it in Redis with a TTL (Time to Live). The data includes stock tickers, bid price, ask price, last trade price, trading volume, and timestamps. The consumer listens to the Kafka topic and caches the data in Redis for quick access.

In this project, you will find three different types of microservices.

1. MarketDataSimulator
2. DataConsumer
3. DataReader

## Installation
``````````````````````````````````````````````````````````````````````````````````
Install Dependencies:


pip install confluent-kafka redis

Run the Producer:
python data_consumer.py


```````````````````````````````````````````````````````````````````````````````````

## License
This project is licensed under the MIT License.

