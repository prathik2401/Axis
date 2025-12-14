"""
Example: Using Axis with Prometheus Metrics and Health Checks

This example demonstrates how to:
1. Start the replication system with HTTP monitoring enabled
2. Query the health endpoints
3. Access Prometheus metrics
4. Integrate with monitoring systems (Prometheus/Grafana)
"""
import asyncio
import aiohttp
from axis.plugins.base import DatabaseConfig
from axis.replication.orchestrator import ReplicationOrchestrator
from axis.replication.state_store import StateStore
from utils.logging import get_logger

logger = get_logger(__name__)


async def query_health_endpoints(port: int = 8080):
    """Query health check endpoints to demonstrate their usage."""
    async with aiohttp.ClientSession() as session:
        # Health check endpoint
        async with session.get(f"http://localhost:{port}/health") as resp:
            health = await resp.json()
            logger.info(f"Health Status: {health['status']}")
            logger.info(f"Components: {list(health['components'].keys())}")
        
        # Readiness check endpoint
        async with session.get(f"http://localhost:{port}/ready") as resp:
            ready = await resp.json()
            logger.info(f"Ready: {ready['ready']}")
        
        # Metrics endpoint (Prometheus format)
        async with session.get(f"http://localhost:{port}/metrics") as resp:
            metrics = await resp.text()
            logger.info(f"Prometheus Metrics (first 500 chars):\n{metrics[:500]}")
        
        # Status endpoint (detailed info)
        async with session.get(f"http://localhost:{port}/status") as resp:
            status = await resp.json()
            logger.info(f"Status: {status}")


async def run_with_monitoring():
    """Run replication system with monitoring enabled."""
    
    # Create source and replica configs
    source_config = DatabaseConfig(
        dsn="postgresql://user:pass@localhost:5432/source_db",
        pool_min_size=2,
        pool_max_size=10,
    )
    
    replica_config = DatabaseConfig(
        dsn="postgresql://user:pass@localhost:5433/replica_db",
        pool_min_size=2,
        pool_max_size=10,
    )
    
    # Create state store
    state_store = StateStore(persistence_path="./axis_state.json")
    
    # Create orchestrator with HTTP server enabled
    orchestrator = ReplicationOrchestrator(
        source_db_config=source_config,
        rabbitmq_url="amqp://guest:guest@localhost:5672/",
        exchange_name="axis_replication",
        channel_name="db_changes",
        state_store=state_store,
        tables_to_replicate=["users", "orders", "products"],
        replica_configs=[replica_config],
        enable_http_server=True,  # Enable HTTP server
        http_port=8080,           # Default port
    )
    
    # Start the orchestrator
    logger.info("Starting replication orchestrator with monitoring...")
    await orchestrator.start()
    
    # Wait a bit for the server to start
    await asyncio.sleep(2)
    
    # Query health endpoints
    logger.info("\n" + "=" * 60)
    logger.info("Querying Health Endpoints")
    logger.info("=" * 60)
    try:
        await query_health_endpoints(8080)
    except Exception as e:
        logger.warning(f"Could not query endpoints: {e}")
    
    # Run for a while
    logger.info("\n" + "=" * 60)
    logger.info("System is running. Press Ctrl+C to stop.")
    logger.info("Visit http://localhost:8080 for monitoring interface")
    logger.info("=" * 60)
    
    try:
        await orchestrator.run_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await orchestrator.stop()


async def prometheus_config_example():
    """
    Example Prometheus scrape configuration.
    
    Add this to your prometheus.yml:
    
    scrape_configs:
      - job_name: 'axis-replication'
        static_configs:
          - targets: ['localhost:8080']
        metrics_path: '/metrics'
        scrape_interval: 15s
    
    Then you can query metrics like:
    - axis_events_received_total
    - axis_events_published_total
    - axis_queue_size
    - axis_backpressure_level
    - axis_component_health
    - axis_replication_lag_seconds
    """
    pass


async def grafana_dashboard_example():
    """
    Example Grafana dashboard queries.
    
    1. Event Throughput:
       rate(axis_events_received_total[5m])
    
    2. Publishing Rate:
       rate(axis_events_published_total[5m])
    
    3. Queue Depth:
       axis_queue_size{queue_type="event_queue"}
    
    4. Error Rate:
       rate(axis_events_failed_total[5m])
    
    5. Backpressure Level:
       axis_backpressure_level
    
    6. Replication Lag:
       axis_replication_lag_seconds{replica="primary"}
    
    7. Component Health:
       axis_component_health
    """
    pass


async def health_check_integration():
    """
    Example: Using health checks in Docker/Kubernetes.
    
    Docker Compose:
    ```yaml
    services:
      axis-replication:
        image: axis:latest
        healthcheck:
          test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
          interval: 30s
          timeout: 10s
          retries: 3
          start_period: 40s
    ```
    
    Kubernetes:
    ```yaml
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
    """
    pass


if __name__ == "__main__":
    """
    To run this example:
    
    1. Ensure PostgreSQL and RabbitMQ are running
    2. Update database DSNs above
    3. Run: python examples/example_with_monitoring.py
    4. Visit http://localhost:8080 for monitoring
    5. Query Prometheus metrics at http://localhost:8080/metrics
    
    Available endpoints:
    - http://localhost:8080/         - Index page
    - http://localhost:8080/health   - Health check (liveness)
    - http://localhost:8080/ready    - Readiness check
    - http://localhost:8080/metrics  - Prometheus metrics
    - http://localhost:8080/status   - Detailed status
    """
    asyncio.run(run_with_monitoring())
