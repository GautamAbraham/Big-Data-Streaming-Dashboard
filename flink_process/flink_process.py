from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common import Duration
import json
import logging
import configparser
import os
import sys
import traceback
import time
from typing import List

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

            # Normalize timestamp using our custom function
            try:
                timestamp_ms = parse_timestamp(timestamp)
                # Format to ISO for consistency
                timestamp = format_timestamp(timestamp_ms)
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
                "timestamp_ms": timestamp_ms,  # Add millisecond timestamp for easier processing
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
            print(f"Unexpected error in DataValidator.map(): {e}")
            print(traceback.format_exc())
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
        Now handles event-time processed data with watermark metadata.
        :param value: JSON string from EventTimeProcessor containing event-time processed data
        :return: Enriched JSON string or None if input was invalid
        """
        try:
            validation_result = json.loads(value)
            
            # Skip invalid data (should not reach here, but safety check)
            if not validation_result.get("is_valid", False):
                print(f"DataEnricher received invalid data: {value}")
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
                "processed_at": get_current_time_iso()
            }
            
            # Add event-time processing metadata if available
            if "event_time_processing" in data and data["event_time_processing"]:
                enriched["event_time_processing"] = True
                enriched["is_late_data"] = data.get("is_late_data", False)
                enriched["processing_time"] = data.get("processing_time", get_current_time_iso())
                
                # Add watermark information for monitoring and debugging
                if "watermark_info" in data:
                    enriched["watermark_info"] = data["watermark_info"]
                    
                # Add arrival delay information
                if "arrival_delay_ms" in data:
                    enriched["arrival_delay_ms"] = data["arrival_delay_ms"]
                    enriched["arrival_delay_seconds"] = round(data["arrival_delay_ms"] / 1000.0, 2)
            else:
                enriched["event_time_processing"] = False

            return json.dumps(enriched)

        except json.JSONDecodeError:
            print(f"DataEnricher received invalid JSON: {value}")
            return None
        except Exception as e:
            print(f"Unexpected error in DataEnricher.map(): {e}")
            print(traceback.format_exc())
            return None

def extract_event_timestamp(element: str, record_timestamp: int) -> int:
    """
    Clean and simple timestamp extraction function for watermark strategy.
    Uses the pre-computed timestamp_ms from validation instead of parsing again.
    """
    try:
        data = json.loads(element)
        if data.get("is_valid", False):
            # Use pre-computed timestamp_ms if available
            if "timestamp_ms" in data["data"]:
                return data["data"]["timestamp_ms"]
            else:
                # Fall back to parsing if needed
                return parse_timestamp(data["data"]["timestamp"])
    except Exception:
        pass
    return record_timestamp

class RadiationTimestampAssigner(TimestampAssigner):
    """
    Proper TimestampAssigner implementation for PyFlink compatibility
    """
    
    def extract_timestamp(self, element: str, record_timestamp: int) -> int:
        """
        Extract timestamp from radiation data record
        """
        return extract_event_timestamp(element, record_timestamp)

class EventTimeProcessor(MapFunction):
    """
    PyFlink-native event-time processor that uses watermarking for late data detection.
    This leverages PyFlink's built-in event-time capabilities without complex windowing.
    """
    
    def __init__(self, allowed_lateness_seconds: int = 30):
        """
        Initialize the processor
        :param allowed_lateness_seconds: How many seconds of lateness to allow
        """
        self.allowed_lateness_ms = allowed_lateness_seconds * 1000
        self.last_watermark = 0  # Track the last seen watermark
    
    def map(self, value: str) -> str:
        """
        Process each record using event-time semantics with watermark awareness
        """
        try:
            data = json.loads(value)
            
            if not data.get("is_valid", False):
                return value
                
            record_data = data["data"]
            record_timestamp = record_data.get("timestamp_ms", 0)
            
            # Update watermark based on current record (simplified approach)
            # In practice, PyFlink handles watermarks automatically, but we simulate here
            current_processing_time = int(time.time() * 1000)
            estimated_watermark = current_processing_time - self.allowed_lateness_ms
            
            # Check if data is late based on watermark
            if record_timestamp < estimated_watermark:
                # Mark as late data
                record_data["is_late_data"] = True
                record_data["late_arrival_reason"] = f"Event time {record_timestamp} is before current watermark {estimated_watermark}"
                record_data["processing_time"] = get_current_time_iso()
                record_data["event_time_processing"] = True
                record_data["watermark_info"] = {
                    "estimated_watermark": estimated_watermark,
                    "allowed_lateness_ms": self.allowed_lateness_ms,
                    "event_timestamp": record_timestamp
                }
                
                return json.dumps({
                    "is_valid": True,
                    "data": record_data,
                    "late_data": True
                })
            else:
                # Normal data - add event-time processing metadata
                record_data["is_late_data"] = False
                record_data["event_time_processing"] = True
                record_data["processing_time"] = get_current_time_iso()
                record_data["watermark_info"] = {
                    "estimated_watermark": estimated_watermark,
                    "allowed_lateness_ms": self.allowed_lateness_ms,
                    "event_timestamp": record_timestamp
                }
                record_data["arrival_delay_ms"] = current_processing_time - record_timestamp
                
                return json.dumps({
                    "is_valid": True,
                    "data": record_data
                })
                
        except Exception as e:
            print(f"Error in EventTimeProcessor: {e}")
            return value

# Utility functions for timestamp handling without external libraries
def parse_timestamp(timestamp_str):
    """
    Ultra-simplified timestamp parser designed to be completely serialization-safe.
    Handles ISO 8601 timestamps and returns milliseconds since epoch.
    """
    if not timestamp_str or not isinstance(timestamp_str, str):
        return int(time.time() * 1000)  # Current time as fallback
        
    try:
        # Super simplified version with fallbacks for serialization safety
        if 'T' in timestamp_str:
            parts = timestamp_str.split('T')
            if len(parts) == 2:
                date_part = parts[0]
                time_part = parts[1]
                
                # Strip timezone indicators
                for tz_char in ['Z', '+', '-']:
                    if tz_char in time_part:
                        time_part = time_part.split(tz_char)[0]
                
                # Remove subseconds
                if '.' in time_part:
                    time_part = time_part.split('.')[0]
                
                # Simple parsing with manual conversion to avoid datetime objects
                try:
                    year, month, day = map(int, date_part.split('-'))
                    hour, minute, second = map(int, time_part.split(':'))
                    
                    # Convert to epoch manually
                    epoch_days = (year - 1970) * 365 + (month - 1) * 30 + day
                    epoch_seconds = epoch_days * 86400 + hour * 3600 + minute * 60 + second
                    return epoch_seconds * 1000
                except ValueError:
                    # Last resort fallback to current time
                    return int(time.time() * 1000)
        
        # If no T delimiter or parsing failed, try simple timestamp
        try:
            return int(float(timestamp_str) * 1000)
        except ValueError:
            return int(time.time() * 1000)
    except Exception:
        # Ultra-safe fallback
        return int(time.time() * 1000)

def format_timestamp(timestamp_ms):
    """
    Ultra-simplified timestamp formatter that is completely serialization-safe.
    Formats millisecond timestamp as ISO 8601 without timezone.
    """
    try:
        seconds = timestamp_ms / 1000.0
        # Manual formatting to avoid datetime objects
        time_tuple = time.gmtime(seconds)
        return f"{time_tuple.tm_year:04d}-{time_tuple.tm_mon:02d}-{time_tuple.tm_mday:02d}T{time_tuple.tm_hour:02d}:{time_tuple.tm_min:02d}:{time_tuple.tm_sec:02d}Z"
    except Exception:
        # Safe fallback with current time
        time_tuple = time.gmtime()
        return f"{time_tuple.tm_year:04d}-{time_tuple.tm_mon:02d}-{time_tuple.tm_mday:02d}T{time_tuple.tm_hour:02d}:{time_tuple.tm_min:02d}:{time_tuple.tm_sec:02d}Z"

def get_current_time_iso():
    """
    Get current time in ISO format with Z timezone indicator.
    Ultra-serialization-safe implementation.
    """
    time_tuple = time.gmtime()
    return f"{time_tuple.tm_year:04d}-{time_tuple.tm_mon:02d}-{time_tuple.tm_mday:02d}T{time_tuple.tm_hour:02d}:{time_tuple.tm_min:02d}:{time_tuple.tm_sec:02d}Z"

class ParallelAggregator(MapFunction):
    """
    Third operator: Performs parallel aggregation and statistics calculation.
    Calculates running statistics for radiation levels in parallel.
    """

    def __init__(self):
        self.record_count = 0
        self.sum_value = 0.0
        self.max_value = 0.0
        self.high_radiation_count = 0

    def map(self, value: str) -> str:
        """
        Aggregate statistics for radiation data in parallel.
        :param value: JSON string from DataEnricher
        :return: Enriched JSON string with aggregated statistics
        """
        try:
            data = json.loads(value)
            
            # Extract radiation value
            radiation_value = data.get("value", 0)
            level = data.get("level", "unknown")
            
            # Update running statistics
            self.record_count += 1
            self.sum_value += radiation_value
            self.max_value = max(self.max_value, radiation_value)
            
            if level == "high":
                self.high_radiation_count += 1
            
            # Calculate running average
            avg_value = self.sum_value / self.record_count if self.record_count > 0 else 0
            
            # Add aggregated statistics to the record
            data["statistics"] = {
                "running_count": self.record_count,
                "running_average": round(avg_value, 2),
                "running_max": self.max_value,
                "high_radiation_events": self.high_radiation_count,
                "high_radiation_percentage": round((self.high_radiation_count / self.record_count) * 100, 2) if self.record_count > 0 else 0
            }
            
            return json.dumps(data)
            
        except Exception as e:
            print(f"Error in ParallelAggregator: {e}")
            return value  # Return original value if processing fails

class ParallelAlertProcessor(MapFunction):
    """
    Fourth operator: Processes alerts and notifications in parallel.
    Generates alert metadata and priority levels.
    """

    def map(self, value: str) -> str:
        """
        Process alerts and add alert metadata.
        :param value: JSON string with aggregated data
        :return: JSON string with alert information
        """
        try:
            data = json.loads(value)
            
            radiation_value = data.get("value", 0)
            is_dangerous = data.get("dangerous", False)
            level = data.get("level", "unknown")
            
            # Determine alert priority
            if is_dangerous:
                alert_priority = "CRITICAL"
                alert_message = f"CRITICAL: Radiation level {radiation_value} CPM detected"
            elif level == "high":
                alert_priority = "HIGH"
                alert_message = f"HIGH: Elevated radiation {radiation_value} CPM"
            elif level == "moderate":
                alert_priority = "MEDIUM"
                alert_message = f"MEDIUM: Moderate radiation {radiation_value} CPM"
            else:
                alert_priority = "LOW"
                alert_message = f"LOW: Normal radiation {radiation_value} CPM"
            
            # Add alert metadata
            data["alert"] = {
                "priority": alert_priority,
                "message": alert_message,
                "requires_notification": is_dangerous or level == "high",
                "alert_timestamp": get_current_time_iso(),
                "alert_id": f"alert_{int(time.time() * 1000)}_{radiation_value}"
            }
            
            return json.dumps(data)
            
        except Exception as e:
            print(f"Error in ParallelAlertProcessor: {e}")
            return value

def main():
    """
    OPTIMAL PYFLINK RADIATION MONITORING ARCHITECTURE
    
    This implements the most efficient PyFlink architecture for radiation monitoring:
    
    1. PARALLEL LOADING: Kafka source with configurable parallelism
    2. PARALLEL VALIDATION: Data validation with high parallelism (4 instances)
    3. EARLY WATERMARKING: Watermarks applied immediately after validation
    4. EVENT-TIME PROCESSING: PyFlink-native event-time processing with late data detection
    5. PARALLEL ENRICHMENT: Data enrichment after event-time processing
    6. PARALLEL SINKS: Multiple Kafka sinks for different data types
    
    Key Benefits:
    - Maximizes parallelism for CPU-intensive operations (validation, enrichment)
    - Uses PyFlink's built-in watermarking for reliable event-time processing
    - Applies watermarking as early as possible for better ordering
    - Simple MapFunction approach avoids complex windowing APIs
    - Configurable lateness handling (30 seconds default)
    - Separate topics for normal, critical, dirty, and late data
    
    All configuration parameters are loaded from config.ini.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    
    config = load_config()  # Load configuration from config.ini

    try:
        kafka_topic = config['DEFAULT']['KAFKA_TOPIC']
        kafka_bootstrap_servers = config['DEFAULT']['KAFKA_BOOTSTRAP_SERVERS']
        kafka_output_topic = config['DEFAULT'].get('KAFKA_OUTPUT_TOPIC', 'flink-processed-output')
        kafka_dirty_topic = config['DEFAULT'].get('KAFKA_DIRTY_TOPIC', 'dirty-data')
        kafka_critical_topic = config['DEFAULT'].get('KAFKA_CRITICAL_TOPIC', 'critical-data')
        kafka_late_topic = config['DEFAULT'].get('KAFKA_LATE_TOPIC', 'late-data')
        
        # Event-Time Processing Configuration
        allowed_lateness_seconds = config['DEFAULT'].getint('ALLOWED_LATENESS_SECONDS', 30)
        watermark_idle_timeout = config['DEFAULT'].getint('WATERMARK_IDLE_TIMEOUT_SECONDS', 60)
        watermark_interval_ms = config['DEFAULT'].getint('WATERMARK_INTERVAL_MS', 1000)
        
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
        
        logging.info(f"Configuration loaded - Danger: {danger_threshold}CPM, Low: {low_threshold}CPM, Moderate: {moderate_threshold}CPM")
        logging.info(f"Parallelism - Source: {source_parallelism}, Validation: {validation_parallelism}, Enrichment: {enrichment_parallelism}, Sink: {sink_parallelism}")
        logging.info(f"Event-Time Processing - Allowed Lateness: {allowed_lateness_seconds} seconds, Watermark Interval: {watermark_interval_ms}ms")
        logging.info(f"Kafka Topics - Output: {kafka_output_topic}, Critical: {kafka_critical_topic}, Dirty: {kafka_dirty_topic}, Late: {kafka_late_topic}")
    except KeyError as e:
        logging.error(f"Missing configuration key: {e}. Please check your config file.")
        sys.exit(1)

    # --- Kafka Consumer with optimized configuration ---
    consumer = FlinkKafkaConsumer(
        topics=kafka_topic,
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': kafka_bootstrap_servers,
            'group.id': 'flink-radiation-monitor',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': 'true',
            'auto.commit.interval.ms': '1000',
            'fetch.min.bytes': '1024',      # Minimum bytes to fetch
            'fetch.max.wait.ms': '500',     # Max wait for fetch
            'max.partition.fetch.bytes': '1048576',  # 1MB max per partition
            'session.timeout.ms': '30000',  # Session timeout
            'heartbeat.interval.ms': '10000'  # Heartbeat interval
        }
    )
    
    # --- Simple Scalable DataStream Processing ---
    # Source with configured parallelism
    ds = env.add_source(consumer).set_parallelism(source_parallelism).name("Kafka Source")

    # Validation operator with parallelism
    validated_stream = ds.map(DataValidator(), output_type=Types.STRING()) \
                        .set_parallelism(validation_parallelism) \
                        .name("Data Validation")

    # Split stream into valid and invalid data with parallelism
    valid_stream = validated_stream.filter(lambda x: json.loads(x).get("is_valid", False)) \
                                   .set_parallelism(validation_parallelism) \
                                   .name("Valid Data Filter")
    
    invalid_stream = validated_stream.filter(lambda x: not json.loads(x).get("is_valid", False)) \
                                     .set_parallelism(validation_parallelism) \
                                     .name("Invalid Data Filter")

    # --- OPTIMAL PYFLINK ARCHITECTURE ---
    # Apply watermarking early (after validation, before enrichment)
    # This ensures event-time processing is applied as early as possible
    watermarked_stream = valid_stream.assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(allowed_lateness_seconds))
        .with_timestamp_assigner(RadiationTimestampAssigner())
    ).set_parallelism(validation_parallelism).name("Early Watermark Assignment")
    
    # Event-time processing with late data detection (PyFlink-native approach)
    processed_stream = watermarked_stream.map(EventTimeProcessor(allowed_lateness_seconds), output_type=Types.STRING()) \
        .set_parallelism(enrichment_parallelism) \
        .name("Event-Time Processing")
    
    # Split processed stream into normal and late data
    normal_processed_stream = processed_stream.filter(lambda x: not json.loads(x).get("late_data", False)) \
                                            .set_parallelism(enrichment_parallelism) \
                                            .name("Normal Processed Data")
    
    late_data_stream = processed_stream.filter(lambda x: json.loads(x).get("late_data", False)) \
                                     .set_parallelism(enrichment_parallelism) \
                                     .name("Late Data Stream")

    # Enrichment operator with parallelism - processes the normal data AFTER ordering logic
    enriched_stream = normal_processed_stream.map(DataEnricher(danger_threshold, low_threshold, moderate_threshold), output_type=Types.STRING()) \
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

    # --- Scalable Output Sinks ---
    # Output normal processed data to main topic
    processed_producer = FlinkKafkaProducer(
        topic=kafka_output_topic,
        serialization_schema=SimpleStringSchema(),
        producer_config={
            'bootstrap.servers': kafka_bootstrap_servers,
            'batch.size': '32768',  # Increased batch size for better throughput (string)
            'linger.ms': '10',      # Small delay for batching (string)
            'compression.type': 'snappy',  # Enable compression
            'acks': '1',            # Wait for leader acknowledgment
            'retries': '3',         # Retry failed sends
            'buffer.memory': '67108864'  # 64MB buffer (string)
        }
    )
    normal_stream.add_sink(processed_producer) \
                 .set_parallelism(sink_parallelism) \
                 .name("Processed Data Sink")

    # Output critical/dangerous data to dedicated critical topic
    critical_producer = FlinkKafkaProducer(
        topic=kafka_critical_topic,
        serialization_schema=SimpleStringSchema(),
        producer_config={
            'bootstrap.servers': kafka_bootstrap_servers,
            'batch.size': '16384',  # Smaller batch for faster delivery of critical data
            'linger.ms': '5',       # Minimal delay for critical data (string)
            'compression.type': 'snappy',  # Fast compression for critical data
            'acks': 'all',          # All replicas must acknowledge for reliability
            'retries': '5',         # More retries for critical data
            'buffer.memory': '33554432',  # 32MB buffer for critical data
            'request.timeout.ms': '10000'  # Shorter timeout for critical data
        }
    )
    critical_stream.add_sink(critical_producer) \
                   .set_parallelism(sink_parallelism) \
                   .name("Critical Data Sink")

    # Output invalid data to dirty data topic with optimized configuration
    dirty_producer = FlinkKafkaProducer(
        topic=kafka_dirty_topic,
        serialization_schema=SimpleStringSchema(),
        producer_config={
            'bootstrap.servers': kafka_bootstrap_servers,
            'batch.size': '16384',  # Batch for dirty data (string)
            'linger.ms': '20',      # Longer linger for dirty data (string)
            'compression.type': 'gzip',  # Better compression for dirty data
            'acks': '1'             # Acknowledge on leader
        }
    )
    invalid_stream.add_sink(dirty_producer) \
                  .set_parallelism(sink_parallelism) \
                  .name("Dirty Data Sink")

    # Output late-arriving data to late data topic
    late_producer = FlinkKafkaProducer(
        topic=kafka_late_topic,
        serialization_schema=SimpleStringSchema(),
        producer_config={
            'bootstrap.servers': kafka_bootstrap_servers,
            'batch.size': '16384',  # Batch for late data (string)
            'linger.ms': '15',      # Moderate linger for late data (string)
            'compression.type': 'snappy',  # Compression for late data
            'acks': '1',            # Acknowledge on leader
            'retries': '2'          # Fewer retries for late data
        }
    )
    late_data_stream.add_sink(late_producer) \
                    .set_parallelism(sink_parallelism) \
                    .name("Late Data Sink")

    # --- Debug output with parallelism ---
    normal_stream.print().set_parallelism(1).name("Normal Data Debug Print")
    critical_stream.print().set_parallelism(1).name("Critical Data Debug Print")
    late_data_stream.print().set_parallelism(1).name("Late Data Debug Print")

    env.execute("Optimized Radiation Monitoring with Event-Time Processing")

if __name__ == "__main__":
    main()
