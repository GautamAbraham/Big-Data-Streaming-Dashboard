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



def create_kafka_producer(bootstrap_servers, config):
    """Create Kafka producer with production-ready configuration and fallback options"""
    while True:
        try:
            # Get compression type with fallback
            compression_type = config['DEFAULT'].get('KAFKA_COMPRESSION', 'gzip')
            
            # Enhanced producer configuration for reliability and performance
            producer_config = {
                'bootstrap_servers': bootstrap_servers,
                'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
                'acks': 'all',  # Wait for all replicas to acknowledge
                'retries': 5,   # Retry failed sends
                'batch_size': int(config['DEFAULT'].get('KAFKA_BATCH_SIZE', 16384)),
                'linger_ms': int(config['DEFAULT'].get('KAFKA_LINGER_MS', 100)),
                'buffer_memory': int(config['DEFAULT'].get('KAFKA_BUFFER_MEMORY', 33554432)),
                'compression_type': compression_type,
                'max_in_flight_requests_per_connection': 1,  # Ensure ordering
            }
            
            try:
                producer = KafkaProducer(**producer_config)
                logging.info(f"Kafka producer created successfully with compression: {compression_type}")
                return producer
            except Exception as compression_error:
                if 'snappy' in str(compression_error).lower():
                    # Fallback to gzip if snappy is not available
                    logging.warning(f"Snappy compression not available: {compression_error}")
                    logging.info("Falling back to gzip compression...")
                    producer_config['compression_type'] = 'gzip'
                    producer = KafkaProducer(**producer_config)
                    logging.info("Kafka producer created successfully with gzip compression")
                    return producer
                else:
                    raise compression_error
            
        except NoBrokersAvailable:
            logging.warning("Kafka broker not available yet. Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            logging.error(f"Error creating Kafka producer: {e}. Retrying in 5 seconds...")
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

def send_data_from_csv(producer: KafkaProducer, topic: str, csv_file_path: str, chunk_size: int = 10000, delay_ms: int = 10):
    """
    Read data from CSV and send raw data to Kafka with minimal validation.
    
    Production Best Practice: 
    - Minimal validation at ingestion (only fatal errors)
    - Let Flink handle business logic validation
    - Focus on high throughput and reliability
    """
    logging.info(f"Starting high-throughput CSV ingestion from {csv_file_path}...")
    
    # Statistics tracking
    stats = {
        'total_rows': 0,
        'sent_rows': 0,
        'skipped_rows': 0,
        'error_rows': 0,
        'start_time': time.time()
    }

    try:
        for chunk_num, chunk in enumerate(pd.read_csv(csv_file_path, chunksize=chunk_size)):
            # Optional: Sort by time for better downstream processing
            if 'Captured Time' in chunk.columns:
                chunk = chunk.sort_values(by='Captured Time')
            
            logging.info(f"Processing chunk {chunk_num + 1} with {len(chunk)} rows")

            for _, row in chunk.iterrows():
                stats['total_rows'] += 1
                
                try:
                    # MINIMAL VALIDATION - Only check for completely empty/null rows
                    if row.isnull().all():
                        stats['skipped_rows'] += 1
                        continue
                    
                    # RAW DATA MAPPING - Simple field mapping without validation
                    # Let Flink handle all business logic validation
                    data = {
                        'captured_time': str(row.get('Captured Time', '')),
                        'latitude': row.get('Latitude', 0),  # Keep as-is, let Flink validate
                        'longitude': row.get('Longitude', 0),  # Keep as-is, let Flink validate
                        'value': row.get('Value', 0),  # Keep as-is, let Flink validate
                        'unit': str(row.get('Unit', '')),  # Keep as-is, let Flink validate
                        'ingestion_timestamp': int(time.time() * 1000)  # Add ingestion time for monitoring
                    }

                    # Use a deduplication key matching Flink's logic: lat|lon|val|captured_time|unit
                    try:
                        lat = round(float(data['latitude']), 5)
                    except Exception:
                        lat = 0.0
                    try:
                        lon = round(float(data['longitude']), 5)
                    except Exception:
                        lon = 0.0
                    try:
                        val = round(float(data['value']), 2)
                    except Exception:
                        val = 0.0
                    ts = str(data.get('captured_time', ''))
                    unit = str(data.get('unit', ''))
                    key_str = f"{lat}|{lon}|{val}|{ts}|{unit}"
                    producer.send(topic, key=key_str.encode('utf-8'), value=data)
                    stats['sent_rows'] += 1

                    # OPTIONAL: Minimal rate limiting for very high throughput
                    if delay_ms > 0:
                        time.sleep(delay_ms / 1000.0)

                except Exception as e:
                    # Only log critical errors, don't validate business logic
                    logging.debug(f"Error processing row: {e}")  # Debug level to avoid log spam
                    stats['error_rows'] += 1
                    continue

            # Log progress every chunk - less frequent for high throughput
            elapsed_time = time.time() - stats['start_time']
            if chunk_num % 10 == 0:  # Log every 10 chunks to reduce overhead
                logging.info(f"Chunk {chunk_num + 1} completed. "
                            f"Sent: {stats['sent_rows']}, "
                            f"Errors: {stats['error_rows']}, "
                            f"Rate: {stats['sent_rows']/elapsed_time:.1f} records/sec")

    except FileNotFoundError:
        logging.error(f"CSV file not found: {csv_file_path}")
        raise
    except Exception as e:
        logging.error(f"Error reading CSV file: {e}")
        raise
    finally:
        producer.flush()
        elapsed_time = time.time() - stats['start_time']
        
        # Final statistics
        logging.info("="*60)
        logging.info("HIGH-THROUGHPUT DATA INGESTION STATISTICS")
        logging.info("="*60)
        logging.info(f"Total rows processed: {stats['total_rows']}")
        logging.info(f"Successfully sent: {stats['sent_rows']}")
        logging.info(f"Skipped (empty): {stats['skipped_rows']}")
        logging.info(f"Errors: {stats['error_rows']}")
        logging.info(f"Success rate: {(stats['sent_rows']/stats['total_rows']*100):.1f}%")
        logging.info(f"Total time: {elapsed_time:.1f} seconds")
        logging.info(f"Average rate: {stats['sent_rows']/elapsed_time:.1f} records/sec")
        logging.info(f"Throughput: {(stats['sent_rows']*len(str(data))/1024/1024/elapsed_time):.1f} MB/sec")
        logging.info("="*60)

def main():
    """Main function with enhanced configuration and error handling"""
    try:
        config = load_config()

        # Load configuration with defaults
        kafka_topic = config['DEFAULT']['KAFKA_TOPIC']
        kafka_bootstrap_servers = config['DEFAULT']['KAFKA_BOOTSTRAP_SERVERS']
        csv_file_path = config['DEFAULT']['CSV_FILE_PATH']
        batch_size = int(config['DEFAULT'].get('BATCH_SIZE', 10000))
        delay_ms = int(config['DEFAULT'].get('SEND_DELAY_MS', 10))

        # Log configuration
        logging.info("="*60)
        logging.info("DATA PROVIDER CONFIGURATION")
        logging.info("="*60)
        logging.info(f"Kafka bootstrap servers: {kafka_bootstrap_servers}")
        logging.info(f"Kafka topic: {kafka_topic}")
        logging.info(f"CSV file path: {csv_file_path}")
        logging.info(f"Batch size: {batch_size}")
        logging.info(f"Send delay: {delay_ms}ms")
        logging.info("="*60)

        # Validate file existence
        if not os.path.exists(csv_file_path):
            logging.error(f"CSV file not found: {csv_file_path}")
            return

        # Create producer and start sending data
        producer = create_kafka_producer(kafka_bootstrap_servers, config)
        send_data_from_csv(producer, kafka_topic, csv_file_path, chunk_size=batch_size, delay_ms=delay_ms)
        
        logging.info("Data provider completed successfully")
        
    except KeyError as e:
        logging.error(f"Missing required configuration: {e}")
        logging.error("Please check your config.ini file")
    except Exception as e:
        logging.error(f"Fatal error in data provider: {e}")
        raise

if __name__ == "__main__":
    main()
