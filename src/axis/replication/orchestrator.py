import asyncio
from typing import List, Dict, Optional, Any
from datetime import datetime

from axis.plugins.base import DatabasePlugin, DatabaseConfig
from axis.plugins.postgres import PostgresPlugin
from axis.consumers.postgres_replay import PostgresReplayConsumer
from axis.messaging.publisher import Publisher
from axis.replication.state_store import StateStore
from axis.replication.backpressure import BackpressureManager, BackpressureConfig, PressureLevel
from axis.monitoring.metrics import get_metrics_collector
from axis.monitoring.health_server import HealthCheckServer
from utils.logging import get_logger

logger = get_logger(__name__)


class ReplicationOrchestrator:
    """
    Orchestrates the entire replication pipeline.
    
    Coordinates:
    - Source database listener
    - RabbitMQ publisher
    - Multiple replica consumers
    - State tracking
    - Backpressure management
    - Health monitoring
    """
    
    def __init__(
        self,
        source_db_config: DatabaseConfig,
        rabbitmq_url: str,
        exchange_name: str,
        channel_name: str,
        state_store: StateStore,
        tables_to_replicate: List[str],
        replica_configs: Optional[List[DatabaseConfig]] = None,
        enable_http_server: bool = True,
        http_port: int = 8080,
    ):
        """
        Initialize replication orchestrator.
        
        Args:
            source_db_config: Source database configuration
            rabbitmq_url: RabbitMQ connection URL
            exchange_name: RabbitMQ exchange name
            channel_name: PostgreSQL NOTIFY channel name
            state_store: State store for tracking
            tables_to_replicate: List of tables to replicate
            replica_configs: Optional list of replica database configs
            enable_http_server: Enable HTTP health check server
            http_port: Port for HTTP server
        """
        self.source_db_config = source_db_config
        self.rabbitmq_url = rabbitmq_url
        self.exchange_name = exchange_name
        self.channel_name = channel_name
        self.state_store = state_store
        self.tables_to_replicate = tables_to_replicate
        self.replica_configs = replica_configs or []
        
        # Components
        self.source_plugin: Optional[PostgresPlugin] = None
        self.publisher: Optional[Publisher] = None
        self.consumers: List[PostgresReplayConsumer] = []
        
        # Monitoring
        self.metrics = get_metrics_collector()
        self.health_server: Optional[HealthCheckServer] = None
        self.enable_http_server = enable_http_server
        self.http_port = http_port
        
        # Backpressure
        self.backpressure = BackpressureManager(
            config=BackpressureConfig(),
            max_queue_size=10000,
            alert_callback=self._handle_pressure_alert,
        )
        
        # Internal queue for events
        self._event_queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
        
        # Worker tasks
        self._worker_task: Optional[asyncio.Task] = None
        self._listener_task: Optional[asyncio.Task] = None
        self._metrics_task: Optional[asyncio.Task] = None
        
        # State
        self._running = False
        self._start_time: Optional[datetime] = None
        
        # Metrics counters
        self._events_received = 0
        self._events_published = 0
        self._errors = 0
    
    async def start(self) -> None:
        """Start the replication orchestrator."""
        logger.info("Starting replication orchestrator...")
        
        try:
            # Load state
            await self.state_store.load_state()
            
            # Connect to source database
            self.source_plugin = PostgresPlugin(self.source_db_config)
            await self.source_plugin.connect()
            logger.info("Connected to source database")
            self.metrics.update_component_health("source_db", True)
            
            # Setup replication on source
            await self.source_plugin.setup_replication(
                tables=self.tables_to_replicate,
                channel=self.channel_name
            )
            logger.info(f"Setup replication for tables: {self.tables_to_replicate}")
            
            # Start RabbitMQ publisher
            self.publisher = Publisher(
                url=self.rabbitmq_url,
                exchange_name=self.exchange_name,
                exchange_type="fanout"
            )
            await self.publisher.start()
            logger.info("Started RabbitMQ publisher")
            self.metrics.update_component_health("rabbitmq", True)
            
            # Start replica consumers
            for i, replica_config in enumerate(self.replica_configs):
                consumer = PostgresReplayConsumer(
                    rabbitmq_url=self.rabbitmq_url,
                    exchange_name=self.exchange_name,
                    queue_name=f"axis_replica_{i}",
                    replica_db_config=replica_config,
                    state_store=self.state_store,
                    batch_size=10,
                )
                await consumer.start()
                self.consumers.append(consumer)
                logger.info(f"Started replica consumer {i}")
                self.metrics.update_component_health(f"consumer_{i}", True)
            
            # Start backpressure monitoring
            await self.backpressure.start_monitoring()
            
            # Start HTTP health check server
            if self.enable_http_server:
                self.health_server = HealthCheckServer(
                    port=self.http_port,
                    health_check_callback=self.health_check
                )
                await self.health_server.start()
            
            # Start worker and listener tasks
            self._worker_task = asyncio.create_task(self._worker_loop())
            self._listener_task = asyncio.create_task(self._listener_loop())
            self._metrics_task = asyncio.create_task(self._metrics_update_loop())
            
            self._running = True
            self._start_time = datetime.now()
            
            logger.info("✓ Replication orchestrator started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start orchestrator: {e}")
            await self.stop()
            raise
    
    async def stop(self) -> None:
        """Stop the replication orchestrator."""
        logger.info("Stopping replication orchestrator...")
        
        self._running = False
        
        # Cancel tasks
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        
        if self._metrics_task:
            self._metrics_task.cancel()
            try:
                await self._metrics_task
            except asyncio.CancelledError:
                pass
        
        # Stop HTTP server
        if self.health_server:
            await self.health_server.stop()
        
        # Stop consumers
        for consumer in self.consumers:
            try:
                await consumer.stop()
            except Exception as e:
                logger.warning(f"Error stopping consumer: {e}")
        
        # Stop backpressure monitoring
        await self.backpressure.stop_monitoring()
        
        # Close publisher
        if self.publisher:
            await self.publisher.close()
        
        # Disconnect from source
        if self.source_plugin:
            await self.source_plugin.disconnect()
        
        logger.info("✓ Replication orchestrator stopped")
    
    async def _listener_loop(self) -> None:
        """Listen for changes from source database."""
        if not self.source_plugin:
            logger.error("Source plugin not initialized")
            return
            
        try:
            async for event in self.source_plugin.listen_changes(self.channel_name):
                self._events_received += 1
                
                # Record metrics
                self.metrics.record_event_received(event.table, event.operation)
                
                # Check backpressure
                if not await self.backpressure.wait_if_needed(timeout=10.0):
                    logger.error("Backpressure timeout - dropping event")
                    self._errors += 1
                    self.metrics.record_event_failed(event.table, event.operation, "backpressure_timeout")
                    continue
                
                # Add to queue
                try:
                    await self._event_queue.put(event)
                    queue_size = self._event_queue.qsize()
                    await self.backpressure.update_queue_size(queue_size)
                    self.metrics.update_queue_size("event_queue", queue_size, self._event_queue.maxsize)
                except asyncio.QueueFull:
                    logger.error("Event queue full - dropping event")
                    self._errors += 1
                    self.metrics.record_event_failed(event.table, event.operation, "queue_full")
                
        except asyncio.CancelledError:
            logger.info("Listener loop cancelled")
        except Exception as e:
            logger.error(f"Error in listener loop: {e}")
            self._errors += 1
    
    async def _worker_loop(self) -> None:
        """Process events from queue and publish to RabbitMQ."""
        if not self.publisher:
            logger.error("Publisher not initialized")
            return
            
        try:
            while self._running:
                # Get event from queue
                event = await self._event_queue.get()
                
                try:
                    # Build message envelope
                    import json
                    envelope = {
                        "source": "db-trigger",
                        "data": {
                            "table": event.table,
                            "schema": event.schema,
                            "operation": event.operation,
                            "payload": event.payload,
                            "old_values": event.old_values,
                            "timestamp": event.timestamp,
                        },
                        "version": 1,
                    }
                    
                    # Publish to RabbitMQ
                    await self.publisher.publish(json.dumps(envelope))
                    self._events_published += 1
                    
                    # Record metrics
                    self.metrics.record_event_published(event.table, event.operation)
                    
                    await self.backpressure.record_success()
                    
                except Exception as e:
                    logger.error(f"Error publishing event: {e}")
                    self._errors += 1
                    self.metrics.record_event_failed(event.table, event.operation, str(e))
                    await self.backpressure.record_failure()
                
                finally:
                    self._event_queue.task_done()
                    queue_size = self._event_queue.qsize()
                    await self.backpressure.update_queue_size(queue_size)
                    self.metrics.update_queue_size("event_queue", queue_size, self._event_queue.maxsize)
                
        except asyncio.CancelledError:
            logger.info("Worker loop cancelled")
        except Exception as e:
            logger.error(f"Error in worker loop: {e}")
    
    async def _handle_pressure_alert(self, level: PressureLevel, message: str) -> None:
        """
        Handle backpressure alerts.
        
        Args:
            level: Pressure level
            message: Alert message
        """
        logger.warning(f"Backpressure alert [{level.value}]: {message}")
        
        # Update metrics
        self.metrics.update_backpressure(level.value)
        
        # Could send notifications, adjust configurations, etc.
        if level == PressureLevel.CRITICAL:
            logger.critical("CRITICAL backpressure - system may be overloaded!")
    
    async def _metrics_update_loop(self) -> None:
        """Periodically update metrics that require computation."""
        try:
            while self._running:
                # Update backpressure metrics
                bp_metrics = await self.backpressure.get_metrics()
                self.metrics.update_backpressure(bp_metrics.get('pressure_level', 'normal'))
                
                # Update circuit breaker state
                circuit_breaker = bp_metrics.get('circuit_breaker', {})
                if isinstance(circuit_breaker, dict):
                    is_open = circuit_breaker.get('state') == 'open'
                    self.metrics.update_circuit_breaker(is_open)
                
                # Update replication lag (if available from state store)
                state_metrics = await self.state_store.get_metrics()
                if 'last_sequence' in state_metrics:
                    # Calculate lag based on pending events
                    lag_seconds = self._event_queue.qsize() * 0.01  # Rough estimate: 10ms per event
                    self.metrics.update_replication_lag("primary", lag_seconds)
                
                # Sleep before next update
                await asyncio.sleep(5.0)  # Update every 5 seconds
                
        except asyncio.CancelledError:
            logger.info("Metrics update loop cancelled")
        except Exception as e:
            logger.error(f"Error in metrics update loop: {e}")
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on all components.
        
        Returns:
            Health status dictionary
        """
        health = {
            'status': 'healthy',
            'components': {},
            'timestamp': datetime.now().isoformat(),
            'uptime_seconds': None,
        }
        
        # Calculate uptime
        if self._start_time:
            uptime = (datetime.now() - self._start_time).total_seconds()
            health['uptime_seconds'] = uptime
        
        # Check source database
        if self.source_plugin:
            source_healthy = await self.source_plugin.health_check()
            health['components']['source_db'] = {
                'healthy': source_healthy,
                'type': self.source_plugin.database_type,
            }
            self.metrics.update_component_health("source_db", source_healthy)
            if not source_healthy:
                health['status'] = 'unhealthy'
        
        # Check consumers
        for i, consumer in enumerate(self.consumers):
            consumer_metrics = await consumer.get_metrics()
            consumer_healthy = consumer.is_running
            health['components'][f'consumer_{i}'] = {
                'healthy': consumer_healthy,
                'metrics': consumer_metrics,
            }
            self.metrics.update_component_health(f"consumer_{i}", consumer_healthy)
            if not consumer_healthy:
                health['status'] = 'degraded'
        
        # Check backpressure
        bp_metrics = await self.backpressure.get_metrics()
        health['components']['backpressure'] = bp_metrics
        
        if bp_metrics['pressure_level'] in ['high', 'critical']:
            health['status'] = 'degraded'
        
        # Check RabbitMQ connection
        rabbitmq_healthy = self.publisher is not None and self._running
        health['components']['rabbitmq'] = {
            'healthy': rabbitmq_healthy,
            'connected': rabbitmq_healthy,
        }
        self.metrics.update_component_health("rabbitmq", rabbitmq_healthy)
        
        # Add overall metrics
        health['metrics'] = {
            'events_received': self._events_received,
            'events_published': self._events_published,
            'errors': self._errors,
            'queue_size': self._event_queue.qsize(),
            'queue_capacity': self._event_queue.maxsize,
        }
        
        return health
    
    async def get_metrics(self) -> Dict[str, Any]:
        """
        Get orchestrator metrics.
        
        Returns:
            Metrics dictionary
        """
        uptime = None
        if self._start_time:
            uptime = (datetime.now() - self._start_time).total_seconds()
        
        state_metrics = await self.state_store.get_metrics()
        bp_metrics = await self.backpressure.get_metrics()
        
        consumer_metrics = []
        for consumer in self.consumers:
            consumer_metrics.append(await consumer.get_metrics())
        
        return {
            'orchestrator': {
                'running': self._running,
                'uptime_seconds': uptime,
                'events_received': self._events_received,
                'events_published': self._events_published,
                'errors': self._errors,
                'queue_size': self._event_queue.qsize(),
            },
            'state_store': state_metrics,
            'backpressure': bp_metrics,
            'consumers': consumer_metrics,
        }
    
    async def run_forever(self) -> None:
        """Run the orchestrator until interrupted."""
        await self.start()
        
        try:
            while self._running:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        finally:
            await self.stop()
