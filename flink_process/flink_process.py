from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction
import json
import logging
import configparser
import os
import sys
import traceback
import math

# Set up basic logging for Flink
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_path: str = "config.ini") -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read(os.getenv("CONFIG_FILE", config_path))
    return config

class CleanKafkaJSON(MapFunction):
    """
    Cleans and validates raw JSON records from Kafka.
    Filters out invalid entries and enriches with danger assessment.
    Accepts only records with 'unit' as 'cpm' (case-insensitive).
    """

    def __init__(self, threshold: float):
        """
        :param threshold: Radiation value threshold above which 'dangerous' is True
        """
        self.threshold = threshold

    def map(self, value: str) -> str:
        """
        Process a raw Kafka message: validate, clean, and enrich it.
        :param value: Raw JSON string from Kafka
        :return: Cleaned JSON string or None if invalid
        """
        try:
            # Log every raw input
            print(f"RAW MESSAGE: {value}")

            data = json.loads(value)

            # Extract and validate numerical fields
            try:
                lat = float(data.get("latitude"))
                lon = float(data.get("longitude"))
                val = float(data.get("value"))
            except (TypeError, ValueError):
                logging.warning(f"Invalid types in record: {data}")
                return None

            # Validate value ranges
            if not (-90 <= lat <= 90) or not (-180 <= lon <= 180) or val <= 0:
                logging.warning(f"Invalid lat/lon/value in record: {data}")
                return None

            # Validate and clean timestamp
            timestamp = data.get("captured_time")
            if not timestamp:
                logging.warning(f"Missing timestamp: {data}")
                return None

            # Unit validation: must be 'cpm' (case-insensitive)
            unit = data.get("unit", "")
            if not isinstance(unit, str) or unit.strip().lower() != "cpm":
                logging.warning(f"Invalid unit (must be 'cpm'): {data}")
                return None

            # Handle loader_id safely
            loader_id = data.get("loader_id", "unknown")
            if loader_id is None or (isinstance(loader_id, float) and math.isnan(loader_id)):
                loader_id = "unknown"

            # Assemble cleaned output
            cleaned = {
                "timestamp": timestamp,
                "lat": lat,
                "lon": lon,
                "value": val,
                "unit": unit.lower(),  # normalize case
                "loader_id": loader_id,
                "dangerous": val > self.threshold
            }

            return json.dumps(cleaned)

        except json.JSONDecodeError:
            logging.error(f"Invalid JSON: {value}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error in map(): {e}")
            logging.error(traceback.format_exc())
            return None

def main():
    """
    Main function to set up the Flink streaming job.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Set parallelism for the Flink job

    config = load_config()  # Load configuration from config.ini

    try:
        kafka_topic = config['DEFAULT']['KAFKA_TOPIC']
        kafka_bootstrap_servers = config['DEFAULT']['KAFKA_BOOTSTRAP_SERVERS']
        # Define the threshold for dangerous radiation levels.
        danger_threshold = 1000.0
    except KeyError as e:
        logging.error(f"Missing configuration key: {e}. Please check your config file.")
        sys.exit(1)

    # --- Kafka Consumer using legacy add_source API ---
    consumer = FlinkKafkaConsumer(
        topics=kafka_topic,
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': kafka_bootstrap_servers,
            'group.id': 'flink-radiation-monitor'
        }
    )
    
    # ---DataStream Processing---
    ds = env.add_source(consumer)

    # ---DataStream Processing---
    # Use the CleanKafkaJSON class, passing the threshold value. 
    cleaned_stream = ds.map(CleanKafkaJSON(danger_threshold), output_type=Types.STRING()) \
                    .filter(lambda x: x is not None)  # Filter out None values (invalid records)
    
    # --- Output for testing ---
    # cleaned_stream.print()

    cleaned_stream.map(lambda x: f"Processed: {x}").print()

    env.execute("Radiation Monitoring Flink Job")

if __name__ == "__main__":
    main()
