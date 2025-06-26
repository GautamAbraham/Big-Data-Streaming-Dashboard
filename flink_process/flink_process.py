from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
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
from datetime import datetime
import dateutil.parser
import pytz

# Set up basic logging for Flink
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_path: str = "config.ini") -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read(os.getenv("CONFIG_FILE", config_path))
    return config

class ValidationResult:
    """
    Container for validation results with valid/invalid data tracking
    """
    def __init__(self, is_valid: bool, data: dict = None, error_message: str = None):
        self.is_valid = is_valid
        self.data = data
        self.error_message = error_message

class DataValidator(MapFunction):
    """
    First operator: Validates raw JSON records from Kafka.
    Returns ValidationResult with valid data or error information.
    """

    def map(self, value: str) -> str:
        """
        Validate raw Kafka message and return validation result.
        :param value: Raw JSON string from Kafka
        :return: JSON string with validation result
        """
        try:
            # Log every raw input
            print(f"RAW MESSAGE: {value}")

            # Parse JSON
            try:
                data = json.loads(value)
            except json.JSONDecodeError:
                result = ValidationResult(False, error_message=f"Invalid JSON: {value}")
                return json.dumps({
                    "is_valid": result.is_valid,
                    "error": result.error_message,
                    "raw_data": value
                })

            # Extract and validate numerical fields
            try:
                lat = float(data.get("latitude"))
                lon = float(data.get("longitude"))
                val = float(data.get("value"))
            except (TypeError, ValueError):
                result = ValidationResult(False, error_message=f"Invalid numeric types in record: {data}")
                return json.dumps({
                    "is_valid": result.is_valid,
                    "error": result.error_message,
                    "raw_data": value
                })

            # Validate value ranges
            if not (-90 <= lat <= 90) or not (-180 <= lon <= 180) or not val or val <= 0:
                result = ValidationResult(False, error_message=f"Invalid lat/lon/value ranges: lat={lat}, lon={lon}, val={val}")
                return json.dumps({
                    "is_valid": result.is_valid,
                    "error": result.error_message,
                    "raw_data": value
                })

            # Validate and clean timestamp
            timestamp = data.get("captured_time")
            if not timestamp:
                result = ValidationResult(False, error_message=f"Missing timestamp in record: {data}")
                return json.dumps({
                    "is_valid": result.is_valid,
                    "error": result.error_message,
                    "raw_data": value
                })

            # Normalize timestamp to ISO 8601 with timezone (UTC)
            try:
                dt = dateutil.parser.isoparse(timestamp)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=pytz.UTC)
                dt_utc = dt.astimezone(pytz.UTC)
                timestamp = dt_utc.isoformat()
            except Exception as e:
                result = ValidationResult(False, error_message=f"Invalid timestamp format: {timestamp}, error: {str(e)}")
                return json.dumps({
                    "is_valid": result.is_valid,
                    "error": result.error_message,
                    "raw_data": value
                })

            # Unit validation: must be 'cpm' (case-insensitive)
            unit = data.get("unit", "")
            if not isinstance(unit, str) or unit.strip().lower() != "cpm":
                result = ValidationResult(False, error_message=f"Invalid unit (must be 'cpm'): {unit}")
                return json.dumps({
                    "is_valid": result.is_valid,
                    "error": result.error_message,
                    "raw_data": value
                })

            # CPM value validation: must be integer (no decimal values allowed)
            if not isinstance(val, int) and not val.is_integer():
                result = ValidationResult(False, error_message=f"CPM value must be integer, got: {val}")
                return json.dumps({
                    "is_valid": result.is_valid,
                    "error": result.error_message,
                    "raw_data": value
                })

            # Convert to integer for consistency
            val = int(val)

            # If all validations pass, return valid data
            valid_data = {
                "timestamp": timestamp,
                "lat": lat,
                "lon": lon,
                "value": val,
                "unit": "cpm"
            }

            result = ValidationResult(True, valid_data)
            return json.dumps({
                "is_valid": result.is_valid,
                "data": result.data
            })

        except Exception as e:
            logging.error(f"Unexpected error in DataValidator.map(): {e}")
            logging.error(traceback.format_exc())
            result = ValidationResult(False, error_message=f"Unexpected error: {str(e)}")
            return json.dumps({
                "is_valid": result.is_valid,
                "error": result.error_message,
                "raw_data": value
            })

class DataEnricher(MapFunction):
    """
    Second operator: Enriches validated data with level classification and danger assessment.
    Only processes valid data from the first operator.
    """

    def __init__(self, threshold: float):
        """
        :param threshold: Radiation value threshold above which 'dangerous' is True
        """
        self.threshold = threshold

    def map(self, value: str) -> str:
        """
        Enrich validated data with level classification and danger assessment.
        :param value: JSON string from DataValidator containing validated data
        :return: Enriched JSON string or None if input was invalid
        """
        try:
            validation_result = json.loads(value)
            
            # Skip invalid data (should not reach here, but safety check)
            if not validation_result.get("is_valid", False):
                logging.warning(f"DataEnricher received invalid data: {value}")
                return None

            data = validation_result["data"]
            val = data["value"]

            # Level classification based on value
            if val < 20:
                level = "low"
            elif 20 <= val < self.threshold:
                level = "moderate"
            else:
                level = "high"
            
            # Assemble enriched output
            enriched = {
                "timestamp": data["timestamp"],
                "lat": round(data["lat"], 5),
                "lon": round(data["lon"], 5),
                "value": val,
                "unit": data["unit"],
                "level": level,
                "dangerous": val > self.threshold,
                "processed_at": datetime.now(pytz.UTC).isoformat()
            }

            return json.dumps(enriched)

        except json.JSONDecodeError:
            logging.error(f"DataEnricher received invalid JSON: {value}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error in DataEnricher.map(): {e}")
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
        kafka_output_topic = config['DEFAULT'].get('KAFKA_OUTPUT_TOPIC', 'flink-processed-output')
        kafka_dirty_topic = config['DEFAULT'].get('KAFKA_DIRTY_TOPIC', 'dirty-data')
        # Define the threshold for dangerous radiation levels.
        danger_threshold = 100.0
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

    # First operator: Data validation
    validated_stream = ds.map(DataValidator(), output_type=Types.STRING())

    # Split stream into valid and invalid data
    valid_stream = validated_stream.filter(lambda x: json.loads(x).get("is_valid", False))
    invalid_stream = validated_stream.filter(lambda x: not json.loads(x).get("is_valid", False))

    # Second operator: Data enrichment (only for valid data)
    enriched_stream = valid_stream.map(DataEnricher(danger_threshold), output_type=Types.STRING()) \
                                  .filter(lambda x: x is not None)

    # --- Output enriched data to processed data topic ---
    processed_producer = FlinkKafkaProducer(
        topic=kafka_output_topic,
        serialization_schema=SimpleStringSchema(),
        producer_config={
            'bootstrap.servers': kafka_bootstrap_servers
        }
    )
    enriched_stream.add_sink(processed_producer)

    # --- Output invalid data to dirty data topic ---
    dirty_producer = FlinkKafkaProducer(
        topic=kafka_dirty_topic,
        serialization_schema=SimpleStringSchema(),
        producer_config={
            'bootstrap.servers': kafka_bootstrap_servers
        }
    )
    invalid_stream.add_sink(dirty_producer)

    # --- Output for testing ---
    # cleaned_stream.print()
    # cleaned_stream.map(lambda x: f"Processed: {x}").print()

    env.execute("Radiation Monitoring Flink Job")

if __name__ == "__main__":
    main()
