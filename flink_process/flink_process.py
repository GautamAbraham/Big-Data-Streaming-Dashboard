from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common.time import Duration
import json
import logging
import configparser
import os
import sys
import time
from datetime import datetime

# Set up basic logging for Flink
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_path: str = "config.ini") -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read(os.getenv("CONFIG_FILE", config_path))
    return config



class DataValidator(MapFunction):
    """
    Validator for radiation data:
    1. JSON parsing
    2. Field existence check
    3. Data type validation
    4. Range validation (lat/lon/value)
    """
    
    def __init__(self):
        # Pre-compile what we can for better performance
        self.required_fields = {'latitude', 'longitude', 'value', 'captured_time', 'unit'}
    
    def map(self, value: str) -> str:
        """
        Simple validation - lightweight and fast.
        """
        try:
            # STAGE 1: Quick JSON parsing
            try:
                data = json.loads(value)
            except json.JSONDecodeError:
                return self._create_error_response("Invalid JSON", value)
            
            # STAGE 2: Field existence check
            if not self.required_fields.issubset(data.keys()):
                missing = self.required_fields - data.keys()
                return self._create_error_response(f"Missing fields: {missing}", value)
            
            # STAGE 3: Basic validation
            try:
                # Basic numeric conversion
                lat = float(data["latitude"])
                lon = float(data["longitude"]) 
                val = float(data["value"])
                
                # Latitude and longitude range validation
                if not (-90 <= lat <= 90):
                    return self._create_error_response(f"Invalid latitude: {lat} (must be between -90 and 90)", value)
                
                if not (-180 <= lon <= 180):
                    return self._create_error_response(f"Invalid longitude: {lon} (must be between -180 and 180)", value)
                
                # Value validation
                if val <= 0:
                    return self._create_error_response(f"Invalid radiation value: {val} (must be positive)", value)
                
                # Unit check
                if str(data["unit"]).lower() != "cpm":
                    return self._create_error_response(f"Invalid unit: {data['unit']}", value)
                
                # Timestamp check and validation
                timestamp = data.get("captured_time")
                if not timestamp:
                    return self._create_error_response("Missing timestamp", value)
                
                # Validate timestamp format
                try:
                    datetime.strptime(str(timestamp), '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    return self._create_error_response(f"Invalid timestamp format: {timestamp} (expected YYYY-MM-DD HH:MM:SS)", value)
                
                # Return validated data (keep val as float, don't convert to int here)
                valid_data = {
                    "timestamp": timestamp,
                    "lat": lat,
                    "lon": lon, 
                    "value": val,
                    "unit": "cpm"
                }
                
                return json.dumps({
                    "is_valid": True,
                    "data": valid_data
                })
                
            except (TypeError, ValueError) as e:
                return self._create_error_response(f"Invalid numeric values: {str(e)}", value)
                
        except Exception as e:
            return self._create_error_response(f"Unexpected error: {str(e)}", value)
    
    def _create_error_response(self, error_msg: str, raw_data: str) -> str:
        """Helper to create consistent error responses"""
        return json.dumps({
            "is_valid": False,
            "error": error_msg,
            "raw_data": raw_data
        })



class DataEnricher(MapFunction):
    """
    Data enricher that handles classification and processing.
    Note: Range validation is now done in DataValidator.
    """
    
    def __init__(self, danger_threshold: float, low_threshold: float, moderate_threshold: float):
        self.danger_threshold = danger_threshold
        self.low_threshold = low_threshold
        self.moderate_threshold = moderate_threshold
    
    def map(self, value: str) -> str:
        try:
            validation_result = json.loads(value)
            
            if not validation_result.get("is_valid", False):
                return None
                
            data = validation_result["data"]
            
            lat = data["lat"]
            lon = data["lon"]
            val = data["value"]
            
            # Convert to integer for consistency if it's a whole number
            val = int(val) if isinstance(val, float) and val.is_integer() else val
            
            # Level classification
            if val < self.low_threshold:
                level = "low"
            elif self.low_threshold <= val < self.moderate_threshold:
                level = "moderate"
            else:
                level = "high"
            
            # Build enriched output
            enriched = {
                "timestamp": data["timestamp"],
                "processing_time": int(time.time() * 1000),  # Processing time in milliseconds
                "lat": round(lat, 5),
                "lon": round(lon, 5),
                "value": val,
                "unit": "cpm",
                "level": level,
                "dangerous": val >= self.danger_threshold
            }
            
            return json.dumps(enriched)
            
        except Exception as e:
            print(f"Error in DataEnricher: {e}")
            return None

def extract_timestamp(json_str: str) -> int:
    """
    Extract timestamp from validated JSON data.
    If parsing fails, return -1 to signal dirty data.
    """
    try:
        data = json.loads(json_str)
        if data.get("is_valid", False):
            timestamp_str = data["data"]["timestamp"]
            try:
                dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                return int(dt.timestamp() * 1000)
            except Exception:
                # Parsing failed, signal dirty data
                logging.warning(f"Failed to parse timestamp: {timestamp_str}")
                return -1
        else:
            # Invalid data, signal dirty data
            return -1
    except Exception:
        # Any error, signal dirty data
        return -1


class RadiationTimestampAssigner(TimestampAssigner):
    """
    Custom timestamp assigner for radiation monitoring data.
    Implements the TimestampAssigner interface required by Flink.
    """
    
    def extract_timestamp(self, element, record_timestamp):
        """
        Extract timestamp from the element.
        Required method for TimestampAssigner interface.
        """
        return extract_timestamp(element)


def create_watermark_strategy(max_out_of_orderness_seconds: int = 20, 
                             idle_timeout_minutes: int = 1) -> WatermarkStrategy:
    """
    Create a watermark strategy for radiation monitoring data.
    
    Args:
        max_out_of_orderness_seconds: Maximum time data can be out of order
        idle_timeout_minutes: Time before marking a source as idle
    
    Returns:
        Configured WatermarkStrategy for event-time processing
    """
    try:
        # Create watermark strategy with bounded out-of-orderness
        strategy = WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(max_out_of_orderness_seconds)
        )
        
        # Handle idle sources (sources that don't emit data for a while)
        strategy = strategy.with_idleness(Duration.of_minutes(idle_timeout_minutes))
        
        # Set custom timestamp assigner using the proper interface
        strategy = strategy.with_timestamp_assigner(RadiationTimestampAssigner())
        
        return strategy
        
    except Exception as e:
        logging.warning(f"Error creating watermark strategy: {e}")
        # Fallback to simple ascending timestamps strategy
        return WatermarkStrategy.for_monotonous_timestamps()

def main():
    """
    PYFLINK RADIATION MONITORING WITH WATERMARKING
    
    A PyFlink pipeline for radiation monitoring with event-time processing:
    
    1. Kafka source
    2. Data validation
    3. Watermarking strategy (handles out-of-order events and idle sources)
    4. Data enrichment and classification
    5. Route to Kafka topics (normal, critical, dirty)
    
    Features:
    - Event-time processing with watermarks
    - Handles out-of-order events (configurable tolerance)
    - Idle source detection and handling
    - Maintains simplicity while adding event-time capabilities
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    
    config = load_config()  # Load configuration from config.ini

    try:
        kafka_topic = config['DEFAULT']['KAFKA_TOPIC']
        kafka_bootstrap_servers = config['DEFAULT']['KAFKA_BOOTSTRAP_SERVERS']
        kafka_output_topic = config['DEFAULT'].get('KAFKA_OUTPUT_TOPIC', 'normal-data')
        kafka_dirty_topic = config['DEFAULT'].get('KAFKA_DIRTY_TOPIC', 'dirty-data')
        kafka_critical_topic = config['DEFAULT'].get('KAFKA_CRITICAL_TOPIC', 'critical-data')
        
        # Parallelism Configuration
        source_parallelism = config['DEFAULT'].getint('SOURCE_PARALLELISM', 2)
        validation_parallelism = config['DEFAULT'].getint('VALIDATION_PARALLELISM', 4)
        enrichment_parallelism = config['DEFAULT'].getint('ENRICHMENT_PARALLELISM', 4)
        sink_parallelism = config['DEFAULT'].getint('SINK_PARALLELISM', 2)
        
        # Set global parallelism
        global_parallelism = config['DEFAULT'].getint('GLOBAL_PARALLELISM', 4)
        env.set_parallelism(global_parallelism)
        
        # Radiation Level Thresholds
        danger_threshold = config['DEFAULT'].getfloat('DANGER_THRESHOLD', 100.0)
        low_threshold = config['DEFAULT'].getint('LOW_THRESHOLD', 20)
        moderate_threshold = config['DEFAULT'].getint('MODERATE_THRESHOLD', 50)
        
        # Watermarking Configuration
        max_out_of_orderness = config['DEFAULT'].getint('MAX_OUT_OF_ORDERNESS_SECONDS', 20)
        idle_timeout_minutes = config['DEFAULT'].getint('IDLE_TIMEOUT_MINUTES', 1)
        
        logging.info(f"Configuration loaded - Danger: {danger_threshold}CPM, Low: {low_threshold}CPM, Moderate: {moderate_threshold}CPM")
        logging.info(f"Watermarking - Max out of order: {max_out_of_orderness}s, Idle timeout: {idle_timeout_minutes}min")
        logging.info(f"Parallelism - Source: {source_parallelism}, Validation: {validation_parallelism}, Enrichment: {enrichment_parallelism}, Sink: {sink_parallelism}")
        logging.info(f"Kafka Topics - Output: {kafka_output_topic}, Critical: {kafka_critical_topic}, Dirty: {kafka_dirty_topic}")
    except KeyError as e:
        logging.error(f"Missing configuration key: {e}. Please check your config file.")
        sys.exit(1)

    # --- Simple Kafka Consumer ---
    consumer = FlinkKafkaConsumer(
        topics=kafka_topic,
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': kafka_bootstrap_servers,
            'group.id': 'flink-radiation-monitor',
            'auto.offset.reset': 'latest'
        }
    )
    
    # --- SIMPLE DATASTREAM PROCESSING ---
    # Source with configured parallelism
    ds = env.add_source(consumer).set_parallelism(source_parallelism).name("Kafka Source")

    # Validation operator
    validated_stream = ds.map(DataValidator(), output_type=Types.STRING()) \
                        .set_parallelism(validation_parallelism) \
                        .name("Data Validation")

    # Split stream into valid and invalid data
    valid_stream = validated_stream.filter(lambda x: json.loads(x).get("is_valid", False)) \
                                   .set_parallelism(validation_parallelism) \
                                   .name("Valid Data Filter")
    
    invalid_stream = validated_stream.filter(lambda x: not json.loads(x).get("is_valid", False)) \
                                     .set_parallelism(validation_parallelism) \
                                     .name("Invalid Data Filter")

    # --- WATERMARKING STRATEGY ---
    # Apply watermarking to valid data for event-time processing
    watermark_strategy = create_watermark_strategy(max_out_of_orderness, idle_timeout_minutes)
    
    # Assign timestamps and watermarks to the valid stream
    valid_stream_with_watermarks = valid_stream.assign_timestamps_and_watermarks(watermark_strategy) \
                                               .set_parallelism(validation_parallelism) \
                                               .name("Timestamp & Watermark Assignment")
    
    # Note: Flink's watermarking will automatically filter out events with negative timestamps (-1)
    # So timestamp_invalid_stream will be minimal - most timestamp failures are caught in validation
    
    logging.info(f"Watermarking enabled - Max out of order: {max_out_of_orderness}s, Idle timeout: {idle_timeout_minutes}min")

    # --- ENRICHMENT ---
    # Process watermarked data (Flink automatically handles timestamp validation)
    enriched_stream = valid_stream_with_watermarks.map(DataEnricher(danger_threshold, low_threshold, moderate_threshold), output_type=Types.STRING()) \
                               .filter(lambda x: x is not None) \
                               .set_parallelism(enrichment_parallelism) \
                               .name("Data Enrichment")

    # Split enriched stream into critical and normal data
    critical_stream = enriched_stream.filter(lambda x: json.loads(x).get("dangerous", False) or json.loads(x).get("level") == "high") \
                                    .set_parallelism(enrichment_parallelism) \
                                    .name("Critical Data Filter")
    
    normal_stream = enriched_stream.filter(lambda x: not (json.loads(x).get("dangerous", False) or json.loads(x).get("level") == "high")) \
                                  .set_parallelism(enrichment_parallelism) \
                                  .name("Normal Data Filter")

    # --- Output Sinks ---
    # Output normal processed data to main topic
    processed_producer = FlinkKafkaProducer(
        topic=kafka_output_topic,
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': kafka_bootstrap_servers}
    )
    normal_stream.add_sink(processed_producer) \
                 .set_parallelism(sink_parallelism) \
                 .name("Processed Data Sink")

    # Output critical/dangerous data to dedicated critical topic
    critical_producer = FlinkKafkaProducer(
        topic=kafka_critical_topic,
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': kafka_bootstrap_servers}
    )
    critical_stream.add_sink(critical_producer) \
                   .set_parallelism(sink_parallelism) \
                   .name("Critical Data Sink")

    # Output invalid data to dirty data topic (includes validation failures and timestamp parsing failures)
    dirty_producer = FlinkKafkaProducer(
        topic=kafka_dirty_topic,
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': kafka_bootstrap_servers}
    )
    
    # Route all dirty data (validation failures) to dirty data sink
    # Note: Since we validate timestamps in DataValidator, most dirty data is caught there
    invalid_stream.add_sink(dirty_producer) \
                  .set_parallelism(sink_parallelism) \
                  .name("Dirty Data Sink")

    # --- Debug output ---
    normal_stream.print().set_parallelism(1).name("Normal Data Debug Print")
    critical_stream.print().set_parallelism(1).name("Critical Data Debug Print")

    env.execute("Simple Radiation Monitoring")

if __name__ == "__main__":
    main()
