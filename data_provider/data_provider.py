import os
import time
import json
import logging
import configparser
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
import pandas as pd
import threading

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# --- GLOBAL for playback speed ---
playback_speed = 1.0

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

def listen_for_speed(bootstrap_servers, config_topic):
    global playback_speed
    consumer = KafkaConsumer(
        config_topic,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='speed-listener-group'
    )
    logging.info("Started playback speed config listener...")
    for msg in consumer:
        try:
            val = msg.value
            if "playback_speed" in val:
                playback_speed = float(val["playback_speed"])
                logging.info(f"Received NEW playback speed: {playback_speed}")
        except Exception as e:
            logging.warning(f"Error parsing playback speed: {e}")

def send_data_from_csv(
    producer: KafkaProducer,
    topic: str,
    csv_file_path: str,
    chunk_size: int = 10000,
):
    global playback_speed
    logging.info(f"Starting to read CSV in chunks from {csv_file_path}...")

    reference_time = None
    wallclock_start = None

    for chunk in pd.read_csv(csv_file_path, chunksize=chunk_size):
        chunk = chunk.sort_values(by="Captured Time")

        for _, row in chunk.iterrows():
            if pd.isna(row.get('Value')) or row.get('Value') <= 0:
                continue

            data_time_str = row.get('Captured Time', '')
            try:
                data_time = pd.to_datetime(data_time_str).timestamp()
            except Exception as e:
                print(f"Error parsing time {data_time_str}: {e}")
                continue 
            if reference_time is None:
                reference_time = data_time
                wallclock_start = time.time()

            # --- THE HEART: Wallclock playback simulation, speed-adjusted ---
            elapsed_reference = (data_time - reference_time) / max(playback_speed, 0.01)
            elapsed_wallclock = time.time() - wallclock_start
            wait_time = elapsed_reference - elapsed_wallclock
            if wait_time > 0:
                time.sleep(wait_time)
            # ---------------------------------------------------------------

            data = {
                'captured_time': data_time,
                'lat': row.get('Latitude', 0),      
                'lon': row.get('Longitude', 0),      
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
    config_topic = config['DEFAULT'].get('CONFIG_TOPIC', 'playback-config')  # <-- new config topic

    # Start background thread to listen for speed
    listener_thread = threading.Thread(
        target=listen_for_speed,
        args=(kafka_bootstrap_servers, config_topic),
        daemon=True
    )
    listener_thread.start()

    producer = create_kafka_producer(kafka_bootstrap_servers)
    send_data_from_csv(producer, kafka_topic, csv_file_path, chunk_size=batch_size)

if __name__ == "__main__":
    main()
