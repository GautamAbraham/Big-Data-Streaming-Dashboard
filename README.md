# Real-Time Radiation Data Visualization System

## Architecture Overview

This system provides real-time visualization of radiation data using a modern streaming architecture:

```
Data Provider → Kafka → Flink → Kafka → FastAPI → WebSocket → React Frontend
```

### Components:

1. **Data Provider**: Reads CSV data and streams to Kafka
2. **Apache Kafka**: Message broker for data streaming
3. **Apache Flink**: Stream processing for data validation and enrichment
4. **FastAPI Backend**: WebSocket server for real-time frontend updates
5. **React Frontend**: Interactive map visualization with Mapbox

## Key Features

-   ✅ Real-time data streaming and processing
-   ✅ Interactive map visualization with radiation levels
-   ✅ Data validation and enrichment in Flink
-   ✅ WebSocket connection with auto-reconnect
-   ✅ System monitoring and health checks
-   ✅ Containerized deployment with Docker
-   ✅ Production-ready configuration with monitoring

## Quick Start

### Prerequisites

-   Docker & Docker Compose
-   Mapbox access token (get from https://mapbox.com)

### Quick Start (Recommended)

**For Windows users**, use the deployment scripts:

```batch
# Development deployment (optimized for 4 vCPUs, 8GB RAM)
.\deploy_scaled_system.bat

# Production deployment
.\deploy_production.bat
```

**For Linux/Mac users**:

```bash
# Development
docker-compose up --build

# Production
docker-compose -f docker-compose.prod.yaml up --build
```

### Development Setup

1. **Clone and navigate to project**:

    ```bash
    cd path/to/project
    ```

2. **Set up environment variables**:

    ```bash
    # Ensure front_end/.env exists with correct values
    # The .env file should contain:
    # VITE_MAPBOX_TOKEN=your_mapbox_token
    # VITE_API_URL=http://localhost:8000
    # VITE_WS_URL=ws://localhost:8000/ws
    ```

3. **Start all services**:

    ```bash
    docker-compose up --build
    ```

4. **Access the application**:
    - Frontend: http://localhost:3000
    - Backend API: http://localhost:8000
    - Flink Dashboard: http://localhost:8081

### Production Deployment

**Windows users**:

```batch
.\deploy_production.bat
```

**Linux/Mac users**:

```bash
docker-compose -f docker-compose.prod.yaml up --build
```

```bash
docker-compose -f docker-compose.prod.yaml up --build
```

Additional services in production:

-   Prometheus metrics: http://localhost:9090
-   Enhanced health checks and auto-restart
-   Data persistence with volumes

## System Architecture Details

### Data Flow

1. **Data Ingestion**: CSV files are read and streamed to Kafka topic `radiation-data`
2. **Stream Processing**: Flink processes data from `radiation-data` topic:
    - Validates coordinates (lat/lon within bounds)
    - Filters invalid records (unit must be 'cpm')
    - Enriches with danger levels and timestamps
    - **Multiple Output Streams**:
        - `processed-data-output` → Clean, valid data for frontend
        - `dirty-data` → Invalid/corrupted data for monitoring
        - `late-data` → Late-arriving data for analysis
3. **Real-time API**: FastAPI consumes processed data and broadcasts via WebSocket
4. **Visualization**: React frontend connects to WebSocket and displays data on interactive map

### Kafka Topics Architecture

| Topic                   | Purpose                        | Consumer                | Priority     |
| ----------------------- | ------------------------------ | ----------------------- | ------------ |
| `radiation-data`        | Raw sensor data input          | Flink (source)          | N/A          |
| `processed-data-output` | Clean, enriched normal data    | Backend → Frontend      | Normal       |
| `critical-data`         | High-priority/dangerous alerts | Backend → Frontend      | **CRITICAL** |
| `dirty-data`            | Invalid/corrupted records      | Monitoring systems      | Low          |
| `late-data`             | Late-arriving measurements     | Data recovery workflows | Low          |

This multi-topic approach ensures:

-   **Data Quality**: Clean separation of valid vs invalid data
-   **Priority Handling**: Critical radiation alerts are processed with higher priority
-   **Real-time Alerting**: Dangerous radiation levels trigger immediate notifications
-   **Monitoring**: Track data quality and rejection rates
-   **Auditing**: Complete data lineage and audit trail
-   **Recovery**: Handle late-arriving or missed data

### Key Components Explained

#### Flink Stream Processing

-   **Validation**: Ensures data quality (valid coordinates, positive values, correct units)
-   **Enrichment**: Adds danger level classification and processing timestamps
-   **Error Handling**: Logs and filters out invalid records
-   **Metrics**: Tracks processing statistics (valid/invalid record counts)

#### Backend WebSocket Server

-   **Connection Management**: Handles multiple concurrent WebSocket connections
-   **Multi-Topic Consumption**: Consumes from both normal and critical data topics
-   **Error Recovery**: Auto-reconnect logic and connection health monitoring
-   **Data Broadcasting**: Efficiently sends data to all connected clients
-   **Priority Handling**: Tags critical data for frontend alerting
-   **Health Endpoint**: `/health` for monitoring system status

#### Critical Data Sink & Alerting

The system implements a dedicated **Critical Data Sink** for high-priority radiation alerts:

**Critical Data Criteria:**

-   `dangerous = true` (radiation ≥ 100 CPM)
-   `level = "high"` (radiation ≥ 50 CPM)

**Critical Data Features:**

-   **Dedicated Kafka Topic**: `critical-data` with optimized configuration
-   **Fast Delivery**: Smaller batch sizes (16KB) and minimal linger time (5ms)
-   **High Reliability**: More retries (5) and all-replica acknowledgment
-   **Priority Tagging**: Backend tags critical data with `"data_priority": "critical"`
-   **Enhanced Logging**: Critical alerts logged with INFO level for monitoring
-   **Frontend Integration**: Critical data can trigger special UI alerts/notifications

**Configuration (Optimized for Speed):**

```
batch.size: 16384 (16KB for faster delivery)
linger.ms: 5 (minimal delay)
compression.type: snappy (fast compression)
acks: all (maximum reliability)
retries: 5 (enhanced fault tolerance)
```

#### Frontend Real-time Visualization

-   **Buffering Strategy**: Batches incoming data for smooth rendering
-   **Performance Optimization**: Limits displayed points and uses efficient updates
-   **Connection Status**: Shows real-time connection status and data statistics
-   **Interactive Map**: Color-coded radiation levels with zoom/pan capabilities

## Monitoring & Observability

### Built-in Monitoring

-   **System Monitor**: Real-time connection status and metrics
-   **Health Checks**: Docker health checks for all services
-   **Data Statistics**: Track processed points and radiation levels
-   **Connection Status**: WebSocket connection monitoring

### Production Monitoring (with docker-compose.prod.yaml)

-   **Prometheus**: Metrics collection from Flink and backend
-   **Flink Metrics**: Processing rates, backpressure, checkpoints
-   **Service Health**: Automated health checks and restarts

### Debugging Tips

1. **Check Flink Processing**:

    ```bash
    # View Flink logs
    docker-compose logs flink_process

    # Access Flink UI
    # http://localhost:8081
    ```

2. **Monitor Backend**:

    ```bash
    # View backend logs
    docker-compose logs backend

    # Check health endpoint
    curl http://localhost:8000/health
    ```

3. **Frontend Debug**:
    - Open browser console for WebSocket connection logs
    - Check network tab for failed requests
    - Monitor data statistics panel

## Configuration

### Environment Variables

The project uses the `.env` file located in the `front_end/` directory for all environment variable configuration.

#### Frontend Environment Variables (`front_end/.env`)

```env
VITE_MAPBOX_TOKEN=your_mapbox_token_here
VITE_API_URL=http://localhost:8000
VITE_WS_URL=ws://localhost:8000/ws
```

**Setup Instructions**:

1. Copy `front_end/.env.example` to `front_end/.env` (if not already present)
2. Update the Mapbox token with your own token from https://mapbox.com
3. Both development and production deployments use this single .env file

#### Backend (config.ini)

```ini
[DEFAULT]
KAFKA_TOPIC=processed-data-output
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
```

### Performance Tuning

#### For High Throughput:

1. **Increase Flink Parallelism**:

    ```yaml
    # In docker-compose.prod.yaml
    taskmanager.numberOfTaskSlots: 8
    parallelism.default: 8
    ```

2. **Optimize Frontend Buffering**:

    ```javascript
    // In MapView.jsx
    setInterval(() => {
      // Process buffer
    }, 50); // Reduce interval for faster updates
    ```

3. **Backend Optimization**:
    - Increase Kafka consumer concurrency
    - Add connection pooling
    - Implement message batching

## Data Format

### Input Data (CSV)

```csv
lat,lon,value,unit,captured_at,device_id
35.6762,139.6503,15.2,cpm,2024-01-01T12:00:00Z,device_001
```

### Processed Output (JSON)

```json
{
  "timestamp": "2024-01-01T12:00:00+00:00",
  "lat": 35.676200,
  "lon": 139.650300,
  "value": 15.20,
  "unit": "cpm",
  "level": "low",
  "dangerous": false,
  "device_id": "device_001",
  "processed_at": "2024-01-01T12:00:01+00:00"
}
```

## Troubleshooting

### Common Issues

1. **No Data Appearing on Map**:

    - Check Kafka topics: `docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092`
    - Verify Flink job is running: http://localhost:8081
    - Check backend logs for WebSocket connections

2. **WebSocket Connection Failed**:

    - Ensure backend is running: `curl http://localhost:8000/health`
    - Check firewall/network settings
    - Verify URL in frontend environment variables

3. **High Memory Usage**:

    - Reduce data retention in frontend (lower point limit)
    - Increase Docker memory limits
    - Optimize Flink checkpointing frequency

4. **Data Loss/Lag**:
    - Check Flink backpressure in dashboard
    - Monitor Kafka consumer lag
    - Increase processing parallelism

## Development

### Adding New Features

1. **Data Enrichment**: Modify `EnhancedCleanKafkaJSON` in `flink_process.py`
2. **Visualization**: Update `MapView.jsx` for new data fields
3. **Monitoring**: Add metrics to `SystemMonitor.jsx`

### Testing

```bash
# Run individual services for testing
docker-compose up kafka backend
docker-compose up frontend --build

# Check service health
curl http://localhost:8000/health
```

### Service Dependencies & Orchestration

The system uses Docker Compose service dependencies to ensure proper startup order:

**Dependency Chain:**

```
Kafka (with health check)
  ↓
┌─── Data Provider (waits for kafka healthy)
├─── Flink JobManager (waits for kafka healthy)
└─── Flink Processor (waits for kafka healthy + jobmanager started)
         ↓
    TaskManagers (wait for jobmanager started)
         ↓
    Backend (waits for jobmanager started)
```

**Benefits:**

-   ✅ **No Manual Waits**: Deployment scripts don't need `timeout` commands
-   ✅ **Automatic Retry**: Services auto-restart if dependencies fail
-   ✅ **Health Checks**: Kafka health check ensures it's ready before dependents start
-   ✅ **Parallel Startup**: Independent services start in parallel where possible
-   ✅ **Graceful Shutdown**: `docker-compose down` stops services in reverse dependency order

**Health Check Configuration:**

-   Kafka includes `nc -z localhost 9092` health check
-   Services use `condition: service_healthy` for Kafka
-   Services use `condition: service_started` for other dependencies

## License

This project is for educational/research purposes. Ensure proper licensing for production use.
