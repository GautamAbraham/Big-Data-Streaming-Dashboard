from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction, ProcessWindowFunction, RuntimeContext
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.time import Time, Duration
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.functions import KeyedProcessFunction
import json
import logging
import configparser
import os
import sys
import time
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TemporalOrderingProcessor(ProcessWindowFunction):
    """
    Implements: "all location information with the same timestamp 
    need to be submitted at the same time"
    
    Groups radiation measurements by capture timestamp and processes them together.
    """
    
    def process(self, key, context, elements):
        try:
            # Convert iterator to list to process all elements with same timestamp
            element_list = list(elements)
            timestamp_key = key
            
            # Process all elements with the same timestamp simultaneously
            batch_output = []
            for element in element_list:
                try:
                    data = json.loads(element)
                    data["temporal_batch_size"] = len(element_list)
                    data["timestamp_key"] = timestamp_key
                    data["temporal_ordering"] = "compliant"
                    
                    batch_output.append(json.dumps(data))
                except Exception as e:
                    logging.warning(f"Error processing element in temporal batch: {e}")
            
            return batch_output
            
        except Exception as e:
            logging.error(f"Error in temporal ordering processor: {e}")
            return []

def extract_timestamp_from_data(json_str: str) -> int:
    """Extract timestamp from radiation data for temporal ordering"""
    try:
        data = json.loads(json_str)
        
        # Get timestamp from processed data
        if "timestamp" in data:
            timestamp_str = data["timestamp"]
        else:
            return None # no timestamp found, return None
            
        try:
            # Normalize timestamp string: remove timezone and microseconds
            clean_timestamp = str(timestamp_str)
            if '+' in clean_timestamp:
                clean_timestamp = clean_timestamp.split('+')[0]
            elif 'Z' in clean_timestamp:
                clean_timestamp = clean_timestamp.replace('Z', '')
            if '.' in clean_timestamp:
                clean_timestamp = clean_timestamp.split('.')[0]
            # Parse timestamp and convert to milliseconds for Flink
            dt = datetime.strptime(clean_timestamp, '%Y-%m-%d %H:%M:%S')
            return int(dt.timestamp() * 1000)
        except Exception:
            return None # no timestamp found, return None
            
    except Exception as e:
        logging.error(f"JSON parsing failed in timestamp extraction: {e}")
        return None # no timestamp found, return None

class RadiationTimestampAssigner(TimestampAssigner):
    """Custom timestamp assigner for temporal ordering"""
    
    def extract_timestamp(self, element, record_timestamp):
        """Extract timestamp from radiation data element"""
        return extract_timestamp_from_data(element)

class RadiationDataProcessor(MapFunction):
    """
    Single-pass radiation data processor
    - Validates input data
    - Enriches with radiation level classification
    - Tags for routing (valid/invalid/critical)
    """
    
    def __init__(self, danger_threshold=1000.0, low_threshold=50, moderate_threshold=200):
        self.danger_threshold = danger_threshold
        self.low_threshold = low_threshold  
        self.moderate_threshold = moderate_threshold
        self.required_fields = {'latitude', 'longitude', 'value', 'captured_time', 'unit'}
        
    def map(self, value):
        """Process radiation data"""
        try:
            # Parse JSON
            data = json.loads(value)
            
            # Validate required fields
            if not self.required_fields.issubset(data.keys()):
                return self._create_invalid(f"Missing fields", value)
            
            # Validate numeric values
            try:
                lat = float(data["latitude"])
                lon = float(data["longitude"])
                radiation_value = float(data["value"])
                
                # Round radiation value to nearest integer
                radiation_value = round(radiation_value)
                
                # Ensure all values are valid numbers
                if not (-90 <= lat <= 90) or not isinstance(lat, (int, float)):
                    return self._create_invalid(f"Invalid latitude: {lat}", value)
                if not (-180 <= lon <= 180) or not isinstance(lon, (int, float)):
                    return self._create_invalid(f"Invalid longitude: {lon}", value)
                if radiation_value <= 0 or not isinstance(radiation_value, int):
                    return self._create_invalid(f"Invalid radiation value: {radiation_value}", value)
                if str(data["unit"]).lower() != "cpm":
                    return self._create_invalid(f"Invalid unit: {data['unit']}", value)
                    
            except (ValueError, TypeError):
                return self._create_invalid("Invalid numeric values", value)
            
            # Classify radiation level
            if radiation_value < self.low_threshold:
                level = "low"
            elif radiation_value < self.moderate_threshold:
                level = "moderate" 
            else:
                level = "high"
            
            # Determine if critical
            is_critical = radiation_value >= self.danger_threshold
            
            # Create enriched output
            enriched = {
                "status": "valid",
                "timestamp": data["captured_time"],
                "lat": round(lat, 5),
                "lon": round(lon, 5),
                "value": radiation_value,
                "unit": "cpm",
                "level": level,
                "critical": is_critical
            }
            
            return json.dumps(enriched)
            
        except json.JSONDecodeError:
            return self._create_invalid("Invalid JSON", value)
        except Exception as e:
            return self._create_invalid(f"Processing error: {str(e)}", value)
    
    def _create_invalid(self, error_msg, raw_data):
        """Create invalid data response"""
        return json.dumps({
            "status": "invalid",
            "error": error_msg,
            "raw_data": raw_data,
            "timestamp": int(time.time() * 1000)
        })

def load_config():
    """Load configuration from config.ini"""
    config = configparser.ConfigParser()
    config.read(os.getenv("CONFIG_FILE", "config.ini"))
    return config

def main():
    """
    Main Flink radiation monitoring pipeline
    """
    # Initialize Flink environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Load configuration
    config = load_config()
    
    try:
        # Kafka configuration
        kafka_topic = config['DEFAULT'].get('KAFKA_TOPIC', 'radiation-data')
        kafka_bootstrap_servers = config['DEFAULT']['KAFKA_BOOTSTRAP_SERVERS']
        kafka_output_topic = config['DEFAULT'].get('KAFKA_OUTPUT_TOPIC', 'normal-data')
        kafka_critical_topic = config['DEFAULT'].get('KAFKA_CRITICAL_TOPIC', 'critical-data')
        kafka_dirty_topic = config['DEFAULT'].get('KAFKA_DIRTY_TOPIC', 'dirty-data')
        
        # Processing thresholds
        danger_threshold = config['DEFAULT'].getfloat('DANGER_THRESHOLD', 1000.0)
        low_threshold = config['DEFAULT'].getint('LOW_THRESHOLD', 50)
        moderate_threshold = config['DEFAULT'].getint('MODERATE_THRESHOLD', 200)
        
        # Temporal ordering configuration - PROJECT REQUIREMENT
        enable_temporal_ordering = config['DEFAULT'].getboolean('ENABLE_TEMPORAL_ORDERING', True)
        temporal_window_seconds = config['DEFAULT'].getint('TEMPORAL_WINDOW_SECONDS', 1)
        watermark_out_of_orderness_seconds = config['DEFAULT'].getint('WATERMARK_OUT_OF_ORDERNESS_SECONDS', 5)
        
        # Parallelism settings
        parallelism = config['DEFAULT'].getint('GLOBAL_PARALLELISM', 4)
        env.set_parallelism(parallelism)
        
        logging.info(f"Starting Flink 2.0 Radiation Monitoring")
        logging.info(f"Thresholds - Danger: {danger_threshold}, Low: {low_threshold}, Moderate: {moderate_threshold}")
        logging.info(f"Temporal Ordering: {'ENABLED' if enable_temporal_ordering else 'DISABLED'} (Window: {temporal_window_seconds}s)")
        logging.info(f"Watermark Tolerance: {watermark_out_of_orderness_seconds}s for out-of-order data")
        logging.info(f"Kafka - Input: {kafka_topic}, Output: {kafka_output_topic}, Critical: {kafka_critical_topic}")
        
    except KeyError as e:
        # Critical configuration missing, exit
        logging.error(f"Missing configuration: {e}")
        sys.exit(1)
    
    # Create Kafka source
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(kafka_bootstrap_servers) \
        .set_topics(kafka_topic) \
        .set_group_id('flink-radiation-monitor') \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    
    # Create data stream with event-time watermarks for out-of-order data  
    # Use proper bounded out-of-orderness strategy
    # Allow 5 seconds for out-of-order events
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)) \
        .with_timestamp_assigner(RadiationTimestampAssigner()) \
        .with_idleness(Duration.of_seconds(30))
    
    raw_stream = env.from_source(kafka_source, watermark_strategy, "Kafka Source")

    # Deduplicate using key_by and stateful ProcessFunction
    def dedup_key_selector(value):
        """
        Generate a unique key for deduplication based on normalized fields.
        Returns a string key or 'invalid_key' if parsing fails.
        """
        try:
            data = json.loads(value)
            lat = round(float(data.get('latitude', 0)), 5)
            lon = round(float(data.get('longitude', 0)), 5)
            val = round(float(data.get('value', 0)), 2)
            ts = str(data.get('captured_time', ''))
            unit = str(data.get('unit', ''))
            return f"{lat}|{lon}|{val}|{ts}|{unit}"
        except Exception as e:
            logging.warning(f"Deduplication key parse error: {e}")
            return "invalid_key"

    class DeduplicateProcessFunction(KeyedProcessFunction):
        """
        Flink KeyedProcessFunction for per-key deduplication using managed state.
        Emits only the first occurrence of each unique key.
        """
        def open(self, runtime_context: RuntimeContext):
            state_desc = ValueStateDescriptor("seen", Types.BOOLEAN())
            self.seen_state = runtime_context.get_state(state_desc)

        def process_element(self, value, ctx):
            if not self.seen_state.value():
                self.seen_state.update(True)
                yield value

    deduped_stream = raw_stream.key_by(dedup_key_selector, key_type=Types.STRING()) \
        .process(DeduplicateProcessFunction(), output_type=Types.STRING()) \
        .name("Deduplicate Records (Keyed State)")

    # Process data
    processed_stream = deduped_stream.map(
        RadiationDataProcessor(danger_threshold, low_threshold, moderate_threshold),
        output_type=Types.STRING()
    ).name("Radiation Data Processor")
    
    # Apply temporal ordering if enabled
    if enable_temporal_ordering:
        logging.info("TEMPORAL ORDERING ENABLED: Event-time grouping with late data tolerance")
        logging.info(f"Window: {temporal_window_seconds}s, Late data tolerance: {watermark_out_of_orderness_seconds}s")
        
        # Filter valid data for temporal ordering
        valid_processed_data = processed_stream.filter(lambda x: '"status": "valid"' in x)
        
        # Group by timestamp key for temporal ordering
        def extract_timestamp_key(json_str: str) -> str:
            """Extract timestamp as string key for grouping"""
            try:
                data = json.loads(json_str)
                timestamp_str = data.get("timestamp", "unknown")
                # Group by second
                if isinstance(timestamp_str, str) and len(timestamp_str) > 19:
                    return timestamp_str[:19]  # YYYY-MM-DD HH:MM:SS
                return str(timestamp_str)
            except:
                return "unknown"
        
        # Apply temporal grouping using event time windows
        # Out-of-order handling is managed by the watermark strategy
        temporally_ordered_stream = valid_processed_data \
            .key_by(extract_timestamp_key) \
            .window(TumblingEventTimeWindows.of(Time.seconds(temporal_window_seconds))) \
            .process(TemporalOrderingProcessor(), output_type=Types.STRING()) \
            .name("Temporal Ordering - Same Timestamp Processing")
        
        # Add invalid data back to the stream (bypasses temporal ordering)
        invalid_processed_data = processed_stream.filter(lambda x: '"status": "invalid"' in x)
        final_stream = temporally_ordered_stream.union(invalid_processed_data)
        
    else:
        logging.info("TEMPORAL ORDERING DISABLED: Standard stream processing")
        final_stream = processed_stream
    
    # Filter streams by status from final stream    
    def is_invalid(json_str):
        try:
            data = json.loads(json_str) 
            return data.get("status") == "invalid"
        except:
            return True
    
    def is_critical(json_str):
        try:
            data = json.loads(json_str)
            return data.get("status") == "valid" and data.get("critical", False)
        except:
            return False
    
    def is_normal(json_str):
        try:
            data = json.loads(json_str)
            return data.get("status") == "valid" and not data.get("critical", False)
        except:
            return False
    
    # Split streams
    invalid_stream = final_stream.filter(is_invalid).name("Invalid Data Filter")
    critical_stream = final_stream.filter(is_critical).name("Critical Data Filter")
    normal_stream = final_stream.filter(is_normal).name("Normal Data Filter")
    
    # Create Kafka sinks with unique transactional ID prefix
    normal_sink = KafkaSink.builder() \
        .set_bootstrap_servers(kafka_bootstrap_servers) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(kafka_output_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
        .set_transactional_id_prefix('normal-sink') \
        .build()
    
    critical_sink = KafkaSink.builder() \
        .set_bootstrap_servers(kafka_bootstrap_servers) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(kafka_critical_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
        .set_transactional_id_prefix('critical-sink') \
        .build()
    
    dirty_sink = KafkaSink.builder() \
        .set_bootstrap_servers(kafka_bootstrap_servers) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(kafka_dirty_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
        .set_transactional_id_prefix('dirty-sink') \
        .build()
    
    # Connect streams to sinks
    normal_stream.sink_to(normal_sink)
    critical_stream.sink_to(critical_sink)
    invalid_stream.sink_to(dirty_sink)
    
    # Debug output
    normal_stream.print()
    critical_stream.print()
    
    # Execute pipeline
    env.execute("Flink Job - Radiation Monitoring Pipeline")

if __name__ == "__main__":
    main()
