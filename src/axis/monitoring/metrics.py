"""
Prometheus metrics collection for Axis replication system.

This module provides a centralized metrics registry and pre-configured
metrics for monitoring the replication pipeline.
"""
from typing import Dict, Any, Optional
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    Summary,
    CollectorRegistry,
    generate_latest,
    CONTENT_TYPE_LATEST,
)
from utils.logging import get_logger

logger = get_logger(__name__)


class MetricsCollector:
    """
    Centralized metrics collector for Axis.
    
    Provides Prometheus metrics for all components of the replication system.
    """
    
    def __init__(self, registry: Optional[CollectorRegistry] = None):
        """
        Initialize metrics collector.
        
        Args:
            registry: Optional Prometheus registry. Uses default if None.
        """
        self.registry = registry or CollectorRegistry()
        self._setup_metrics()
    
    def _setup_metrics(self) -> None:
        """Setup all Prometheus metrics."""
        
        # ============================================
        # Event Metrics
        # ============================================
        self.events_received = Counter(
            'axis_events_received_total',
            'Total number of change events received from source database',
            ['table', 'operation'],
            registry=self.registry
        )
        
        self.events_published = Counter(
            'axis_events_published_total',
            'Total number of events published to RabbitMQ',
            ['table', 'operation'],
            registry=self.registry
        )
        
        self.events_consumed = Counter(
            'axis_events_consumed_total',
            'Total number of events consumed from RabbitMQ',
            ['consumer', 'table'],
            registry=self.registry
        )
        
        self.events_replayed = Counter(
            'axis_events_replayed_total',
            'Total number of events successfully replayed to replicas',
            ['consumer', 'table', 'operation'],
            registry=self.registry
        )
        
        self.events_failed = Counter(
            'axis_events_failed_total',
            'Total number of failed event processing attempts',
            ['component', 'table', 'error_type'],
            registry=self.registry
        )
        
        # ============================================
        # Queue Metrics
        # ============================================
        self.queue_size = Gauge(
            'axis_queue_size',
            'Current size of event queue',
            ['queue_type'],
            registry=self.registry
        )
        
        self.queue_utilization = Gauge(
            'axis_queue_utilization_ratio',
            'Queue utilization as ratio of max size (0.0 to 1.0)',
            ['queue_type'],
            registry=self.registry
        )
        
        # ============================================
        # Batch Metrics
        # ============================================
        self.batch_size = Histogram(
            'axis_batch_size',
            'Size of batches processed',
            ['consumer'],
            buckets=[1, 5, 10, 25, 50, 100, 250, 500],
            registry=self.registry
        )
        
        self.batch_processing_duration = Histogram(
            'axis_batch_processing_seconds',
            'Time taken to process a batch',
            ['consumer'],
            buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0],
            registry=self.registry
        )
        
        # ============================================
        # Backpressure Metrics
        # ============================================
        self.backpressure_level = Gauge(
            'axis_backpressure_level',
            'Current backpressure level (0=normal, 1=moderate, 2=high, 3=critical)',
            registry=self.registry
        )
        
        self.rate_limit_hits = Counter(
            'axis_rate_limit_hits_total',
            'Number of times rate limiting was triggered',
            registry=self.registry
        )
        
        self.circuit_breaker_state = Gauge(
            'axis_circuit_breaker_open',
            'Circuit breaker state (0=closed, 1=open)',
            registry=self.registry
        )
        
        # ============================================
        # Database Metrics
        # ============================================
        self.db_connections_active = Gauge(
            'axis_db_connections_active',
            'Number of active database connections',
            ['database', 'pool'],
            registry=self.registry
        )
        
        self.db_query_duration = Histogram(
            'axis_db_query_duration_seconds',
            'Database query execution time',
            ['database', 'operation'],
            buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
            registry=self.registry
        )
        
        # ============================================
        # Replication Lag
        # ============================================
        self.replication_lag = Gauge(
            'axis_replication_lag_seconds',
            'Replication lag in seconds',
            ['replica'],
            registry=self.registry
        )
        
        self.replication_lag_events = Gauge(
            'axis_replication_lag_events',
            'Number of events behind (approximate)',
            ['replica'],
            registry=self.registry
        )
        
        # ============================================
        # State Store Metrics
        # ============================================
        self.checkpoints_saved = Counter(
            'axis_checkpoints_saved_total',
            'Total number of checkpoints saved',
            registry=self.registry
        )
        
        self.events_tracked = Gauge(
            'axis_events_tracked',
            'Number of events currently being tracked in state store',
            ['status'],
            registry=self.registry
        )
        
        # ============================================
        # Health Metrics
        # ============================================
        self.component_health = Gauge(
            'axis_component_health',
            'Component health status (0=unhealthy, 1=healthy)',
            ['component'],
            registry=self.registry
        )
        
        self.uptime_seconds = Gauge(
            'axis_uptime_seconds',
            'Time since system started',
            registry=self.registry
        )
        
        # ============================================
        # RabbitMQ Metrics
        # ============================================
        self.rabbitmq_messages_sent = Counter(
            'axis_rabbitmq_messages_sent_total',
            'Total messages sent to RabbitMQ',
            ['exchange'],
            registry=self.registry
        )
        
        self.rabbitmq_messages_received = Counter(
            'axis_rabbitmq_messages_received_total',
            'Total messages received from RabbitMQ',
            ['queue'],
            registry=self.registry
        )
        
        logger.info("Prometheus metrics initialized")
    
    def export_metrics(self) -> bytes:
        """
        Export metrics in Prometheus format.
        
        Returns:
            Metrics in Prometheus text format
        """
        return generate_latest(self.registry)
    
    def get_content_type(self) -> str:
        """
        Get content type for metrics endpoint.
        
        Returns:
            Prometheus content type
        """
        return CONTENT_TYPE_LATEST
    
    def record_event_received(self, table: str, operation: str) -> None:
        """Record an event received from source database."""
        self.events_received.labels(table=table, operation=operation).inc()
    
    def record_event_published(self, table: str, operation: str) -> None:
        """Record an event published to RabbitMQ."""
        self.events_published.labels(table=table, operation=operation).inc()
    
    def record_event_consumed(self, consumer: str, table: str) -> None:
        """Record an event consumed from RabbitMQ."""
        self.events_consumed.labels(consumer=consumer, table=table).inc()
    
    def record_event_replayed(
        self, consumer: str, table: str, operation: str
    ) -> None:
        """Record a successful event replay."""
        self.events_replayed.labels(
            consumer=consumer, table=table, operation=operation
        ).inc()
    
    def record_event_failed(
        self, component: str, table: str, error_type: str
    ) -> None:
        """Record a failed event processing."""
        self.events_failed.labels(
            component=component, table=table, error_type=error_type
        ).inc()
    
    def update_queue_size(self, queue_type: str, size: int, max_size: int) -> None:
        """Update queue size metrics."""
        self.queue_size.labels(queue_type=queue_type).set(size)
        utilization = size / max_size if max_size > 0 else 0
        self.queue_utilization.labels(queue_type=queue_type).set(utilization)
    
    def update_backpressure(self, level: str) -> None:
        """Update backpressure level."""
        level_map = {'normal': 0, 'moderate': 1, 'high': 2, 'critical': 3}
        self.backpressure_level.set(level_map.get(level, 0))
    
    def update_circuit_breaker(self, is_open: bool) -> None:
        """Update circuit breaker state."""
        self.circuit_breaker_state.set(1 if is_open else 0)
    
    def update_component_health(self, component: str, is_healthy: bool) -> None:
        """Update component health status."""
        self.component_health.labels(component=component).set(1 if is_healthy else 0)
    
    def update_replication_lag(self, replica: str, lag_seconds: float) -> None:
        """Update replication lag."""
        self.replication_lag.labels(replica=replica).set(lag_seconds)
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """
        Get a summary of current metrics.
        
        Returns:
            Dictionary with current metric values
        """
        # This is a simplified summary - in production you'd query the registry
        return {
            "metrics_collected": True,
            "registry": "prometheus",
            "endpoint": "/metrics"
        }


# Global metrics instance
_metrics_collector: Optional[MetricsCollector] = None


def get_metrics_collector() -> MetricsCollector:
    """
    Get the global metrics collector instance.
    
    Returns:
        MetricsCollector instance
    """
    global _metrics_collector
    if _metrics_collector is None:
        _metrics_collector = MetricsCollector()
    return _metrics_collector


def reset_metrics() -> None:
    """Reset the global metrics collector (useful for testing)."""
    global _metrics_collector
    _metrics_collector = None
