# Real-Time Radiation Data Streaming & Visualization System

## Overview

This project demonstrates a complete, production-style streaming data pipeline. It covers:

-   Real-time data ingestion from CSV to Kafka
-   Distributed messaging and partitioning with Kafka
-   Stateful, parallel stream processing and enrichment with Flink
-   Multi-stream output (clean, critical, dirty data) for robust data quality and alerting
-   Real-time API and WebSocket broadcasting with FastAPI
-   Live, interactive visualization with a modern React/Mapbox frontend

## Architecture

```
Data Provider (CSV → Kafka)
   ↓
Kafka (radiation-data topic)
   ↓
Flink (validation, enrichment, deduplication, multi-stream output)
   ↓
Kafka (processed-data-output, critical-data, dirty-data)
   ↓
FastAPI Backend (WebSocket API)
   ↓
React Frontend (Mapbox visualization)
```

## Components

### 1. Data Provider (`data_provider/`)

-   Reads large CSV files in chunks.
-   Streams records to Kafka (`radiation-data` topic) with a composite key for partitioning.
-   Configurable batch size and send delay for throughput control.
-   Uses Python's `kafka-python` and pandas.

### 2. Kafka (`docker-compose.yaml`)

-   Central message broker.
-   Configured for multiple partitions for parallelism.
-   Exposes UI for monitoring (Kafka UI).

### 3. Flink Stream Processor (`flink_process/`)

-   Consumes from Kafka.
-   Deduplicates, validates, and enriches data.
-   Splits output into:
    -   `processed-data-output` (valid, normal data)
    -   `critical-data` (dangerous readings)
    -   `dirty-data` (invalid/corrupt records)
-   Uses event-time processing and temporal ordering.
-   Parallelism and checkpointing are configurable.

### 4. Backend API (`backend/`)

-   FastAPI server.
-   Consumes processed Kafka topics.
-   Broadcasts data to frontend via WebSocket.
-   Health check endpoint for monitoring.

### 5. Frontend (`front_end/`)

-   React + Vite + Mapbox.
-   Real-time map visualization of radiation data.
-   Receives data via WebSocket.
-   UI components for alerts, system status, and configuration.

---

## Key Features

-   Real-time, end-to-end streaming from CSV to interactive map.
-   Data validation, deduplication, and enrichment in Flink.
-   Multi-topic Kafka output for clean, critical, and dirty data.
-   WebSocket-based backend/frontend communication.
-   Docker Compose orchestration for easy deployment.
-   Health checks and monitoring (Kafka UI, Flink UI).

---

## How to Run

1. **Clone the repository** and navigate to the project root.
2. **Configure environment variables** in `front_end/.env` with the help of `.env.example` provided.
3. **Start all services**:
    ```
    docker-compose up --build
    ```
4. **Access UIs**:
    - Frontend: http://localhost:3000
    - Flink Dashboard: http://localhost:8081
    - Kafka UI: http://localhost:8080
    - Backend API: http://localhost:8000

---

## Configuration

-   **Kafka**: Number of partitions, retention, and performance settings in `docker-compose.yaml`.
-   **Flink**: Parallelism, checkpointing, and memory in `flink_process/config.ini` and Docker Compose.
-   **Data Provider**: Batch size, send delay, and file path in `data_provider/config.ini`.
-   **Frontend**: Mapbox and API URLs in `front_end/.env`.

---

## Developer Notes

-   **Partitioning**: The data provider uses a composite key (lat, lon, value, timestamp, unit) to match Flink's deduplication logic, ensuring correct parallelism and stateful processing.
-   **Consumer Offsets**: Flink manages Kafka consumer offsets using the group ID set in the Kafka source.
-   **Monitoring**: Use Flink UI and Kafka UI for real-time system health and lag monitoring.

---

## Troubleshooting

-   **No data on map**: Check Flink and backend logs, ensure Kafka topics exist, verify WebSocket connection.
-   **High consumer lag**: Check Flink parallelism, TaskManager count, and Kafka partitioning.
-   **Data skew**: Ensure the data provider's key matches Flink's deduplication key.

---

## End-Notes

-   This project demonstrates a complete, production-style streaming data pipeline.
-   It covers ingestion, distributed messaging, real-time processing, multi-stream output, and live visualization.

---
