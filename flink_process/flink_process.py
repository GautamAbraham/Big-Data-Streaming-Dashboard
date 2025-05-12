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

# Set up basic logging for Flink
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_path: str = "config.ini") -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read(os.getenv("CONFIG_FILE", config_path))
    return config

class CleanKafkaJSON(MapFunction):
    """
    Flink MapFunction that parses and validates radiation data from Kafka.
    Filters out malformed or invalid entries and flags dangerous values.
    """

    def __init__(self, threshold: float):
        """
        Initializes the transformation with a danger threshold.
        :param threshold: Radiation level above which a reading is marked dangerous.
        """
        super().__init__()
        self.threshold = threshold

    def map(self, value: str) -> str:
        """
        Parses a JSON string from Kafka, validates fields, and constructs a clean JSON record.
        :param value: A raw JSON string representing the input data.
        :return: A cleaned JSON string, or None if invalid.
        """
        try:
            data = json.loads(value)

            # Extract and validate fields
            lat = float(data.get("latitude", 0))
            lon = float(data.get("longitude", 0))
            val = float(data.get("value", 0))

            if not (-90 <= lat <= 90) or not (-180 <= lon <= 180) or val <= 0:
                logging.warning(f"Dropping invalid record: {data}")
                return None

            # Add additional fields
            result = {
                "timestamp": data.get("captured_time"),
                "lat": lat,
                "lon": lon,
                "value": val,
                "unit": data.get("unit", "unknown"),
                "loader_id": data.get("loader_id", "unknown"),
                "dangerous": val > self.threshold
            }

            return json.dumps(result)

        except Exception as e:
            logging.error(f"Error in map(): {e}\nTraceback:\n{traceback.format_exc()}\nRaw value: {value}")
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
