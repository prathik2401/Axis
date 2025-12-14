# Monitoring Module Documentation

The Axis monitoring module provides comprehensive observability through Prometheus metrics and HTTP health check endpoints.

## Components

### 1. MetricsCollector (`axis/monitoring/metrics.py`)

Centralized Prometheus metrics collection for the entire replication system.

#### Features
- Thread-safe singleton pattern for global access
- Comprehensive metric types (Counter, Gauge, Histogram)
- Organized into logical categories

#### Metrics Categories

**Event Metrics:**
```python
metrics.record_event_received(table="users", operation="INSERT")
metrics.record_event_published(table="users", operation="INSERT")
metrics.record_event_consumed(consumer="consumer_0", table="users")
metrics.record_event_replayed(table="users", operation="INSERT", success=True, duration=0.05)
metrics.record_event_failed(table="users", operation="INSERT", error_type="connection_error")
```

**Queue Metrics:**
```python
metrics.update_queue_size(queue_type="event_queue", size=100, max_size=10000)
```

**Batch Metrics:**
```python
metrics.record_batch_processed(consumer="consumer_0", batch_size=10, duration=0.5, success=True)
```

**Backpressure Metrics:**
```python
metrics.update_backpressure(level="high")  # normal, moderate, high, critical
metrics.update_circuit_breaker(is_open=True)
```

**Database Metrics:**
```python
# Automatically recorded with context manager
with metrics.measure_db_query():
    await db.execute(query)

metrics.update_db_connections(active=5, max_connections=10)
```

**Component Health:**
```python
metrics.update_component_health(component="source_db", is_healthy=True)
metrics.update_component_health(component="rabbitmq", is_healthy=True)
metrics.update_component_health(component="consumer_0", is_healthy=True)
```

**Replication Lag:**
```python
metrics.update_replication_lag(replica="primary", lag_seconds=0.5)
```

#### Usage

```python
from axis.monitoring.metrics import get_metrics_collector

# Get the singleton instance
metrics = get_metrics_collector()

# Record events
metrics.record_event_received("users", "INSERT")
metrics.record_event_published("users", "INSERT")

# Update status
metrics.update_component_health("source_db", True)
metrics.update_backpressure("normal")
```

### 2. HealthCheckServer (`axis/monitoring/health_server.py`)

HTTP server providing health check and metrics endpoints using aiohttp.

#### Endpoints

**GET /**
- Returns HTML index page with links to all endpoints
- Useful for browser-based monitoring

**GET /health**
- Liveness probe - indicates if the service is running
- Returns: `{"status": "healthy"|"unhealthy"|"degraded", ...}`
- Status codes:
  - 200: Healthy
  - 503: Unhealthy or degraded

**GET /ready**
- Readiness probe - indicates if the service is ready to accept traffic
- Returns: `{"ready": true|false, ...}`
- Status codes:
  - 200: Ready
  - 503: Not ready

**GET /metrics**
- Prometheus metrics endpoint
- Returns: Text format Prometheus metrics
- Content-Type: `text/plain; version=0.0.4`

**GET /status**
- Detailed system status
- Returns: Full JSON with all component details
- Includes: uptime, metrics, component health

#### Usage

```python
from axis.monitoring.health_server import HealthCheckServer

async def my_health_check():
    return {
        "status": "healthy",
        "components": {
            "db": {"healthy": True},
            "rabbitmq": {"healthy": True}
        }
    }

# Create and start server
server = HealthCheckServer(
    port=8080,
    health_check_callback=my_health_check
)

await server.start()

# Later, stop the server
await server.stop()
```

## Integration with ReplicationOrchestrator

The orchestrator automatically integrates monitoring:

```python
orchestrator = ReplicationOrchestrator(
    source_db_config=source_config,
    rabbitmq_url=rabbitmq_url,
    exchange_name=exchange_name,
    channel_name=channel_name,
    state_store=state_store,
    tables_to_replicate=["users", "orders"],
    replica_configs=[replica_config],
    enable_http_server=True,  # Enable HTTP monitoring
    http_port=8080,           # HTTP server port
)

await orchestrator.start()  # Starts HTTP server automatically
```

The orchestrator:
1. Records all event metrics automatically
2. Updates queue metrics in real-time
3. Updates component health based on actual state
4. Calculates and updates replication lag
5. Updates backpressure metrics

## Docker/Kubernetes Integration

### Docker Compose

```yaml
services:
  axis-replication:
    image: axis:latest
    ports:
      - "8080:8080"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 40s
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: axis-replication
spec:
  template:
    spec:
      containers:
      - name: axis
        image: axis:latest
        ports:
        - containerPort: 8080
          name: http
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 5
```

### Service Monitor (Prometheus Operator)

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: axis-replication
spec:
  selector:
    matchLabels:
      app: axis-replication
  endpoints:
  - port: http
    path: /metrics
    interval: 15s
```

## Prometheus Configuration

### prometheus.yml

```yaml
scrape_configs:
  - job_name: 'axis-replication'
    static_configs:
      - targets: ['localhost:8080']
    metrics_path: '/metrics'
    scrape_interval: 15s
    scrape_timeout: 10s
```

### Alerting Rules

```yaml
groups:
  - name: axis_alerts
    rules:
      - alert: AxisHighErrorRate
        expr: rate(axis_events_failed_total[5m]) > 10
        for: 5m
        annotations:
          summary: "High error rate in Axis replication"
          
      - alert: AxisHighBackpressure
        expr: axis_backpressure_level >= 2
        for: 5m
        annotations:
          summary: "High backpressure detected"
          
      - alert: AxisComponentUnhealthy
        expr: axis_component_health == 0
        for: 2m
        annotations:
          summary: "Component {{ $labels.component }} is unhealthy"
          
      - alert: AxisHighReplicationLag
        expr: axis_replication_lag_seconds > 60
        for: 5m
        annotations:
          summary: "Replication lag exceeds 60 seconds"
```

## Grafana Dashboards

### Example Queries

**Event Throughput:**
```promql
rate(axis_events_received_total[5m])
```

**Publishing Rate:**
```promql
rate(axis_events_published_total[5m])
```

**Error Rate:**
```promql
rate(axis_events_failed_total[5m])
```

**Queue Depth:**
```promql
axis_queue_size{queue_type="event_queue"}
```

**Backpressure Level:**
```promql
axis_backpressure_level
```

**Replication Lag:**
```promql
axis_replication_lag_seconds{replica="primary"}
```

**Component Health:**
```promql
axis_component_health
```

**Database Query P95:**
```promql
histogram_quantile(0.95, rate(axis_db_query_duration_seconds_bucket[5m]))
```

## Best Practices

1. **Always enable HTTP server in production** for health checks
2. **Set appropriate scrape intervals** (15s recommended)
3. **Configure alerting rules** for critical metrics
4. **Monitor replication lag** to detect performance issues
5. **Track error rates** to identify reliability problems
6. **Use component health** for automated failover decisions
7. **Monitor backpressure** to prevent system overload
8. **Track queue sizes** to identify bottlenecks

## Troubleshooting

### HTTP Server Won't Start

```python
# Check if port is already in use
# Try a different port
orchestrator = ReplicationOrchestrator(
    ...
    http_port=9090  # Try different port
)
```

### Metrics Not Appearing

```python
# Ensure metrics are being recorded
from axis.monitoring.metrics import get_metrics_collector
metrics = get_metrics_collector()
metrics.record_event_received("test_table", "INSERT")

# Check /metrics endpoint
curl http://localhost:8080/metrics | grep axis
```

### Health Check Always Fails

```python
# Check individual components
import aiohttp

async with aiohttp.ClientSession() as session:
    async with session.get('http://localhost:8080/status') as resp:
        status = await resp.json()
        print(status)  # Inspect detailed status
```

## Examples

See [examples/example_with_monitoring.py](../examples/example_with_monitoring.py) for a complete working example.
