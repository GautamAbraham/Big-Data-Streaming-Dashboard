# Scaling Configuration for 4 vCPUs, 8GB RAM

This document describes the optimization and scaling configuration for running the radiation monitoring system efficiently on 4 vCPUs and 8GB RAM.

## Resource Allocation Strategy

### Total System Resources

-   **Target Hardware**: 4 vCPUs, 8GB RAM
-   **Resource Distribution**: Optimized for stream processing workload
-   **Parallelism Strategy**: 6 total task slots across 2 TaskManagers

### Service Resource Allocation

| Service              | Memory Limit | CPU Limit     | Memory Reserve | CPU Reserve   | Purpose                         |
| -------------------- | ------------ | ------------- | -------------- | ------------- | ------------------------------- |
| **Flink JobManager** | 1.5GB        | 1.0 vCPU      | 1.0GB          | 0.5 vCPU      | Job coordination, checkpointing |
| **TaskManager-1**    | 2.0GB        | 1.5 vCPU      | 1.5GB          | 1.0 vCPU      | Stream processing (3 slots)     |
| **TaskManager-2**    | 2.0GB        | 1.5 vCPU      | 1.5GB          | 1.0 vCPU      | Stream processing (3 slots)     |
| **Kafka**            | 1.0GB        | 0.5 vCPU      | 0.5GB          | 0.3 vCPU      | Message broker                  |
| **Backend**          | 0.5GB        | 0.3 vCPU      | 0.3GB          | 0.2 vCPU      | API & WebSocket                 |
| **Data Provider**    | 0.3GB        | 0.2 vCPU      | 0.1GB          | 0.1 vCPU      | Data ingestion                  |
| **Frontend**         | 0.3GB        | 0.2 vCPU      | 0.1GB          | 0.1 vCPU      | Web UI                          |
| **Total**            | **~7.6GB**   | **~4.2 vCPU** | **~5.0GB**     | **~3.2 vCPU** |

## Flink Parallelism Configuration

### Task Slot Distribution

-   **Total Task Slots**: 6 (3 per TaskManager)
-   **Global Parallelism**: 6
-   **Source Parallelism**: 2 (matches Kafka partitions)
-   **Processing Parallelism**: 6 (validation, enrichment)
-   **Sink Parallelism**: 3 (output distribution)

### Memory Configuration

```bash
# JobManager
JOB_MANAGER_MEMORY_PROCESS_SIZE=1536m
JOB_MANAGER_MEMORY_JVM_HEAP_SIZE=1024m
JOB_MANAGER_MEMORY_JVM_METASPACE_SIZE=256m

# TaskManager
TASK_MANAGER_MEMORY_PROCESS_SIZE=2048m
TASK_MANAGER_MEMORY_FRAMEWORK_HEAP_SIZE=128m
TASK_MANAGER_MEMORY_TASK_HEAP_SIZE=1536m
TASK_MANAGER_MEMORY_MANAGED_SIZE=256m
```

## Kafka Optimization

### Topic Configuration

-   **Partitions**: 6 (matches total task slots)
-   **Replication Factor**: 1 (single broker setup)
-   **Log Retention**: 24 hours
-   **Segment Size**: 1GB

### Producer Optimization

```bash
# Main topic (processed data)
batch.size=32768
linger.ms=10
compression.type=snappy
buffer.memory=67108864

# Dirty data topic
batch.size=16384
linger.ms=20
compression.type=gzip
```

### Consumer Optimization

```bash
fetch.min.bytes=1024
fetch.max.wait.ms=500
max.partition.fetch.bytes=1048576
session.timeout.ms=30000
```

## Performance Monitoring

### Key Metrics to Monitor

1. **CPU Utilization**: Target <80% across all cores
2. **Memory Usage**: Target <85% of allocated memory
3. **Kafka Throughput**: Messages/second in/out
4. **Flink Backpressure**: Should be minimal
5. **Task Slot Utilization**: Should be balanced

### Monitoring Commands

```bash
# Resource usage monitoring
python monitor_performance.py

# Flink metrics
curl http://localhost:8081/jobs
curl http://localhost:8081/taskmanagers

# Docker stats
docker stats --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}"
```

## Deployment Instructions

### Development Environment

```bash
# Start scaled development environment
start_scaled_system.bat

# Or manually
docker-compose up --build -d
```

### Production Environment

```bash
# Deploy production configuration
deploy_production.bat

# Or manually
docker-compose -f docker-compose.prod.yaml up --build -d
```

### Flink Job Submission

```bash
# Submit the processing job
docker-compose exec flink-processor python /opt/flink/usrlib/flink_process.py
```

## Performance Tuning Guidelines

### When CPU Usage is High (>90%)

1. Reduce parallelism in config.ini
2. Increase batch sizes for producers
3. Optimize operator logic
4. Check for hot partitions

### When Memory Usage is High (>90%)

1. Reduce TaskManager heap sizes
2. Increase checkpoint interval
3. Tune managed memory settings
4. Check for memory leaks

### When Throughput is Low

1. Increase parallelism (if resources allow)
2. Optimize Kafka settings
3. Reduce operator complexity
4. Check network bottlenecks

### When Latency is High

1. Reduce batch sizes
2. Decrease linger times
3. Optimize watermark strategy
4. Check processing logic

## Configuration Files

### Key Configuration Files

-   `docker-compose.yaml`: Development scaling configuration
-   `docker-compose.prod.yaml`: Production scaling configuration
-   `flink_process/config.ini`: Flink job parallelism and thresholds
-   `monitor_performance.py`: Resource monitoring script

### Environment Variables

```bash
# Set before deployment
export VITE_MAPBOX_TOKEN=your_token
export VITE_API_URL=http://localhost:8000
export VITE_WS_URL=ws://localhost:8000
```

## Troubleshooting

### Common Issues

1. **Out of Memory**: Reduce memory allocation or increase swap
2. **Task Slots Full**: Reduce parallelism or add TaskManager
3. **Kafka Connection Issues**: Check bootstrap servers configuration
4. **Slow Processing**: Check operator performance and parallelism

### Health Checks

```bash
# Check all services
docker-compose ps

# Check specific service logs
docker-compose logs -f [service-name]

# Check Flink dashboard
open http://localhost:8081

# Check backend health
curl http://localhost:8000/health
```

## Expected Performance

### Throughput Targets

-   **Input Rate**: 1000-10000 messages/second
-   **Processing Latency**: <100ms end-to-end
-   **CPU Utilization**: 60-80% average
-   **Memory Usage**: 70-85% allocation
-   **Availability**: >99.5% uptime

### Scaling Limits

-   **Maximum Parallelism**: 6 (limited by task slots)
-   **Maximum Memory**: ~7.6GB allocated
-   **Maximum CPU**: ~4 cores utilized
-   **Kafka Partitions**: 6 (optimal for current setup)

This configuration provides optimal resource utilization while maintaining system stability and performance for a 4 vCPU, 8GB RAM environment.
