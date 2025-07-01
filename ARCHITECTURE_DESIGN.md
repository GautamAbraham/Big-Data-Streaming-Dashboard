# PyFlink Radiation Monitoring Architecture Design

## Overview

This document describes the optimal PyFlink architecture implemented for robust, resource-efficient, and reliable real-time radiation monitoring with proper event-time processing.

## Architecture Principles

### 1. Maximize Parallelism Where It Matters

-   **Validation**: 4 parallel instances for CPU-intensive JSON parsing and validation
-   **Enrichment**: 4 parallel instances for classification and metadata addition
-   **Sources/Sinks**: 2 parallel instances each for I/O operations
-   **Global Parallelism**: 4 instances for overall job coordination

### 2. PyFlink-Native Event-Time Processing

-   **Early Watermarking**: Applied immediately after validation, before enrichment
-   **Simple MapFunction**: Uses straightforward MapFunction instead of complex windowing
-   **Built-in Watermark Strategy**: Leverages PyFlink's `WatermarkStrategy.for_bounded_out_of_orderness()`
-   **No Complex Windows**: Avoids PyFlink's limited windowing APIs that can cause issues

### 3. Robust Late Data Handling

-   **Configurable Lateness**: 30 seconds default, configurable via `config.ini`
-   **Dedicated Late Data Topic**: Separate Kafka topic for late-arriving data
-   **Watermark-Based Detection**: Uses watermark progression for accurate late data detection
-   **Metadata Enrichment**: Adds detailed timing and watermark information

## Pipeline Flow

```
Kafka Source (parallel=2)
    ↓
Data Validation (parallel=4)
    ↓
[SPLIT: Valid vs Invalid]
    ↓
Watermark Assignment (parallel=4) ← EARLY WATERMARKING
    ↓
Event-Time Processing (parallel=4) ← LATE DATA DETECTION
    ↓
[SPLIT: Normal vs Late Data]
    ↓
Data Enrichment (parallel=4) ← AFTER EVENT-TIME PROCESSING
    ↓
[SPLIT: Critical vs Normal]
    ↓
Multiple Kafka Sinks (parallel=2 each)
```

## Key Design Decisions

### Why Early Watermarking?

-   **Immediate Event-Time Context**: Records get event-time semantics as soon as they're validated
-   **Better Ordering**: Watermarks help maintain event-time order throughout the pipeline
-   **Simplified Downstream Logic**: Enrichment and other operators can rely on established watermarks

### Why MapFunction Instead of Windowing?

-   **PyFlink Limitations**: PyFlink's windowing APIs have known limitations and complexity
-   **Simpler Logic**: MapFunction provides cleaner, more maintainable late data detection
-   **Better Performance**: Avoids overhead of window creation and management
-   **Direct Control**: Full control over late data logic without framework constraints

### Why Enrichment After Event-Time Processing?

-   **Timing Accuracy**: Late data detection must happen before enrichment to be meaningful
-   **Metadata Preservation**: Event-time metadata is preserved through enrichment
-   **Logical Flow**: Process timing first, then add business logic (classification, alerts)

## Configuration

All parameters are configurable in `flink_process/config.ini`:

```ini
# Event-Time Processing
ALLOWED_LATENESS_SECONDS = 30
WATERMARK_IDLE_TIMEOUT_SECONDS = 60
WATERMARK_INTERVAL_MS = 1000

# Parallelism
GLOBAL_PARALLELISM = 4
SOURCE_PARALLELISM = 2
VALIDATION_PARALLELISM = 4
ENRICHMENT_PARALLELISM = 4
SINK_PARALLELISM = 2

# Thresholds
DANGER_THRESHOLD = 100.0
LOW_THRESHOLD = 20
MODERATE_THRESHOLD = 50
```

## Benefits of This Architecture

### Performance

-   **Maximum CPU Utilization**: High parallelism for CPU-intensive operations
-   **Efficient I/O**: Optimized Kafka producer/consumer configurations
-   **Minimal Overhead**: Simple operators without complex state management

### Reliability

-   **Event-Time Guarantees**: Proper watermarking ensures event-time correctness
-   **Late Data Recovery**: Late data is captured and sent to dedicated topic
-   **Error Isolation**: Invalid data is separated and sent to dirty data topic

### Maintainability

-   **Simple Operators**: Each operator has a single, clear responsibility
-   **PyFlink-Native**: Uses PyFlink's built-in capabilities effectively
-   **Configurable**: All parameters externalized to configuration file
-   **Well-Documented**: Clear code structure with extensive comments

### Scalability

-   **Resource Efficient**: Configurable parallelism based on available resources
-   **Topic Separation**: Different data types go to appropriate topics
-   **Batch Optimization**: Kafka producers configured for optimal batching

## Monitoring and Debugging

The architecture provides extensive metadata for monitoring:

-   **Watermark Information**: Tracks watermark progression and timing
-   **Arrival Delays**: Measures how late data arrives
-   **Processing Times**: Records when each stage processes data
-   **Event-Time Flags**: Clearly identifies event-time vs processing-time logic

## Future Enhancements

-   **Backpressure Handling**: Add explicit backpressure management
-   **State Backends**: Consider RocksDB for stateful operations if needed
-   **Checkpointing**: Configure checkpointing for fault tolerance
-   **Metrics**: Add custom metrics for business KPIs
