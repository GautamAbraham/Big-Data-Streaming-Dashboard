import os
import time
import json
import logging
import configparser
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def load_config(config_path: str = "config.ini") -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read(os.getenv("CONFIG_FILE", config_path))
    return config



def create_kafka_producer(bootstrap_servers):
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            )
            return producer
        except NoBrokersAvailable:
            print("Kafka broker not available yet. Retrying in 5 seconds...")
            time.sleep(5)


def send_data_from_csv(producer: KafkaProducer, topic: str, csv_file_path: str, chunk_size: int = 10000):
    """Read data from a CSV file and send it to a Kafka topic."""
    logging.info(f"Starting to read CSV in chunks from {csv_file_path}...")

    for chunk in pd.read_csv(csv_file_path, chunksize=chunk_size):
        chunk = chunk.sort_values(by='Captured Time')  # Ensure data is sorted by time

        for _, row in chunk.iterrows():
            # Skip rows with invalid or missing values
            if pd.isna(row.get('Value')) or row.get('Value') <= 0:
                continue

            data = {
                'captured_time': row.get('Captured Time', time.time()),
                'latitude': row.get('Latitude', 0),
                'longitude': row.get('Longitude', 0),
                'value': row.get('Value', 0),
                'unit': row.get('Unit', 'unknown'),
                'loader_id': row.get('Loader ID', 'unknown')
            }

            producer.send(topic, value=data)
            logging.info(f"Sent data to kafka: {data}")

            # OPTIMIZED FOR 8GB RAM: Balanced throughput for 3K-5K records/second
            # Slight delay to prevent overwhelming the system while maintaining high throughput
            time.sleep(0.0002)  # 200 microseconds - allows ~5K records/second

    producer.flush()
    logging.info("Finished sending all data.")

def main():
    config = load_config()

    kafka_topic = config['DEFAULT']['KAFKA_TOPIC']
    kafka_bootstrap_servers = config['DEFAULT']['KAFKA_BOOTSTRAP_SERVERS']
    csv_file_path = config['DEFAULT']['CSV_FILE_PATH']
    batch_size = int(config['DEFAULT'].get('BATCH_SIZE', 10000))

    logging.info(f"Kafka bootstrap servers: {kafka_bootstrap_servers}")
    logging.info(f"Kafka topic: {kafka_topic}")
    logging.info(f"CSV file path: {csv_file_path}")

    producer = create_kafka_producer(kafka_bootstrap_servers)
    send_data_from_csv(producer, kafka_topic, csv_file_path, chunk_size=batch_size)

if __name__ == "__main__":
    main()
