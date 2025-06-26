from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common import Duration
import json
import logging
import configparser
import os
import sys
import traceback
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

    def __init__(self, danger_threshold: float, low_threshold: float, moderate_threshold: float):
        """
        :param danger_threshold: Radiation value threshold above which 'dangerous' is True
        :param low_threshold: Threshold between low and moderate levels
        :param moderate_threshold: Threshold between moderate and high levels
        """
        self.danger_threshold = danger_threshold
        self.low_threshold = low_threshold
        self.moderate_threshold = moderate_threshold

    def map(self, value: str) -> str:
        """
        Enrich validated data with level classification and danger assessment.
        Now handles windowed data with additional metadata.
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

            # Level classification based on configurable thresholds
            if val < self.low_threshold:
                level = "low"
            elif self.low_threshold <= val < self.moderate_threshold:
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
                "dangerous": val >= self.danger_threshold,
                "processed_at": datetime.now(pytz.UTC).isoformat()
            }
            
            # Add windowing metadata if available
            if "window_start" in data:
                enriched["window_start"] = data["window_start"]
                enriched["window_end"] = data["window_end"]
                enriched["watermark_timestamp"] = data["watermark_timestamp"]
                enriched["records_in_window"] = data["records_in_window"]
                enriched["is_late_data"] = data.get("is_late_data", False)
                enriched["event_time_processing"] = True
            else:
                enriched["event_time_processing"] = False

            return json.dumps(enriched)

        except json.JSONDecodeError:
            logging.error(f"DataEnricher received invalid JSON: {value}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error in DataEnricher.map(): {e}")
            logging.error(traceback.format_exc())
            return None

class TimestampExtractor:
    """
    Extract timestamp from valid records for watermark generation
    """
    @staticmethod
    def extract_timestamp(record_json: str, current_timestamp: int) -> int:
        """
        Extract timestamp from valid JSON record and convert to milliseconds
        :param record_json: JSON string with validated data (guaranteed to be valid)
        :param current_timestamp: Current processing time timestamp
        :return: Timestamp in milliseconds since epoch
        """
        try:
            data = json.loads(record_json)
            
            # For valid data, we know the structure is {"is_valid": true, "data": {...}}
            if "is_valid" in data and data["is_valid"]:
                timestamp_str = data["data"]["timestamp"]
                
                # Parse ISO timestamp and convert to milliseconds
                dt = dateutil.parser.isoparse(timestamp_str)
                return int(dt.timestamp() * 1000)
            else:
                # This shouldn't happen since we only apply watermarks to valid data
                logging.warning(f"TimestampExtractor received invalid data: {record_json}")
                return current_timestamp
            
        except Exception as e:
            logging.warning(f"Failed to extract timestamp from {record_json}: {e}")
            # Return current time as fallback
            return current_timestamp

class WindowedDataProcessor(ProcessWindowFunction):
    """
    Process windowed valid data and add window metadata.
    Only processes valid records that have passed validation.
    Handles late data detection and routing.
    """
    
    def process(self, key, context, elements, out):
        """
        Process windowed valid elements and add window metadata
        """
        try:
            window_start = context.window().start
            window_end = context.window().end
            current_watermark = context.current_watermark()
            
            records_in_window = list(elements)
            
            for record in records_in_window:
                try:
                    data = json.loads(record)
                    
                    # All records here should be valid since we filter before windowing
                    if "is_valid" in data and data["is_valid"]:
                        enriched_data = data["data"].copy()
                        
                        # Extract record timestamp for late data detection
                        record_timestamp = dateutil.parser.isoparse(enriched_data["timestamp"])
                        record_timestamp_ms = int(record_timestamp.timestamp() * 1000)
                        
                        # Check if this is late data (arrived after window end + allowed lateness)
                        # TODO: Make this configurable by passing allowed_lateness_minutes to WindowedDataProcessor
                        is_late = record_timestamp_ms < (window_end - 120000)  # 2 minutes allowed lateness (should match config)
                        
                        enriched_data["window_start"] = datetime.fromtimestamp(window_start/1000, pytz.UTC).isoformat()
                        enriched_data["window_end"] = datetime.fromtimestamp(window_end/1000, pytz.UTC).isoformat()
                        enriched_data["watermark_timestamp"] = datetime.fromtimestamp(current_watermark/1000, pytz.UTC).isoformat()
                        enriched_data["records_in_window"] = len(records_in_window)
                        enriched_data["is_late_data"] = is_late
                        enriched_data["record_timestamp_ms"] = record_timestamp_ms
                        
                        if is_late:
                            # Send late data to side output
                            enriched_data["late_arrival_reason"] = f"Record timestamp {record_timestamp_ms} is before window end {window_end} - allowed lateness"
                            context.output("late-data", json.dumps({"is_valid": True, "data": enriched_data, "late_data": True}))
                        else:
                            # Send normal data to main output
                            out.collect(json.dumps({"is_valid": True, "data": enriched_data}))
                    else:
                        # This shouldn't happen, but log it for debugging
                        logging.warning(f"WindowedDataProcessor received invalid data: {record}")
                        
                except Exception as e:
                    logging.error(f"Error processing windowed record: {e}")
                    # Skip malformed records in window
                    
        except Exception as e:
            logging.error(f"Error in WindowedDataProcessor: {e}")
            # Skip all records on severe error

def main():
    """
    Main function to set up the Flink streaming job with watermark strategy.
    All configuration parameters are now loaded from config.ini including:
    - Window size and allowed lateness for event-time processing
    - All radiation level thresholds (danger, low, moderate)
    - Watermark strategy parameters (out-of-orderness and interval)
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Set parallelism for the Flink job
    
    config = load_config()  # Load configuration from config.ini

    try:
        kafka_topic = config['DEFAULT']['KAFKA_TOPIC']
        kafka_bootstrap_servers = config['DEFAULT']['KAFKA_BOOTSTRAP_SERVERS']
        kafka_output_topic = config['DEFAULT'].get('KAFKA_OUTPUT_TOPIC', 'flink-processed-output')
        kafka_dirty_topic = config['DEFAULT'].get('KAFKA_DIRTY_TOPIC', 'dirty-data')
        kafka_late_topic = config['DEFAULT'].get('KAFKA_LATE_TOPIC', 'late-data')
        
        # Windowing Configuration
        window_size_minutes = config['DEFAULT'].getint('WINDOW_SIZE_MINUTES', 5)
        allowed_lateness_minutes = config['DEFAULT'].getint('ALLOWED_LATENESS_MINUTES', 2)
        
        # Radiation Level Thresholds
        danger_threshold = config['DEFAULT'].getfloat('DANGER_THRESHOLD', 100.0)
        low_threshold = config['DEFAULT'].getint('LOW_THRESHOLD', 20)
        moderate_threshold = config['DEFAULT'].getint('MODERATE_THRESHOLD', 50)
        
        # Watermark Configuration
        max_out_of_orderness = config['DEFAULT'].getint('MAX_OUT_OF_ORDERNESS_MS', 30000)
        watermark_interval = config['DEFAULT'].getint('WATERMARK_INTERVAL_MS', 5000)
        
        # Configure watermark strategy and late data handling
        env.get_config().set_auto_watermark_interval(watermark_interval)
        
        logging.info(f"Configuration loaded - Window: {window_size_minutes}min, Lateness: {allowed_lateness_minutes}min, Danger: {danger_threshold}CPM, Low: {low_threshold}CPM, Moderate: {moderate_threshold}CPM")
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

    # Apply watermark strategy using Duration API (fixes PyFlink compatibility)
    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_millis(max_out_of_orderness)) \
        .with_timestamp_assigner(TimestampExtractor.extract_timestamp)
    
    watermarked_valid_stream = valid_stream.assign_timestamps_and_watermarks(watermark_strategy)

    # Apply windowing with allowed lateness for late data handling using Duration API
    windowed_stream_with_late_data = watermarked_valid_stream \
        .window_all(TumblingEventTimeWindows.of(Duration.of_minutes(window_size_minutes))) \
        .allowed_lateness(Duration.of_minutes(allowed_lateness_minutes)) \
        .process(WindowedDataProcessor(), output_type=Types.STRING())

    # Extract late data side output
    late_data_stream = windowed_stream_with_late_data.get_side_output("late-data")

    # Second operator: Data enrichment (only for valid windowed data)
    enriched_stream = windowed_stream_with_late_data.map(DataEnricher(danger_threshold, low_threshold, moderate_threshold), output_type=Types.STRING()) \
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

    # --- Output late data to late data topic ---
    late_producer = FlinkKafkaProducer(
        topic=kafka_late_topic,
        serialization_schema=SimpleStringSchema(),
        producer_config={
            'bootstrap.servers': kafka_bootstrap_servers
        }
    )
    late_data_stream.add_sink(late_producer)

    # --- Output for testing ---
    # enriched_stream.print()

    env.execute("Radiation Monitoring Flink Job with Watermarks and Late Data Handling")

if __name__ == "__main__":
    main()
