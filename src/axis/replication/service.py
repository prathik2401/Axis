"""
Main replication service using the orchestrator.

This is the primary entry point for running the Axis replication system.
"""
import asyncio
from typing import List, Optional
from config.settings import Settings
from axis.plugins.base import DatabaseConfig
from axis.replication.orchestrator import ReplicationOrchestrator
from axis.replication.state_store import StateStore
from utils.logging import get_logger

logger = get_logger(__name__)


async def run(
    settings: Settings,
    replica_dsns: Optional[List[str]] = None,
    enable_http_server: bool = True,
    http_port: int = 8080
):
    """
    Run the replication service using the orchestrator.
    
    Args:
        settings: Application settings
        replica_dsns: Optional list of replica database DSNs
        enable_http_server: Whether to enable HTTP health check server
        http_port: Port for HTTP server
    """
    logger.info("Starting Axis replication service...")
    
    # Create source database config
    source_config = DatabaseConfig(
        dsn=settings.pg_dsn,
        pool_min_size=2,
        pool_max_size=10,
        timeout=30,
    )
    
    # Create replica configs (if provided)
    replica_configs = []
    if replica_dsns:
        for dsn in replica_dsns:
            replica_configs.append(
                DatabaseConfig(
                    dsn=dsn,
                    pool_min_size=2,
                    pool_max_size=10,
                )
            )
        logger.info(f"Configured {len(replica_configs)} replica(s)")
    else:
        logger.warning("No replica databases configured - running in publish-only mode")
    
    # Create state store with persistence
    state_store = StateStore(persistence_path="./axis_state.json")
    
    # Determine tables to replicate (you can make this configurable)
    tables_to_replicate = getattr(settings, 'tables_to_replicate', ['*'])
    if tables_to_replicate == ['*']:
        logger.info("Replicating ALL tables (set tables_to_replicate in settings to limit)")
    else:
        logger.info(f"Replicating tables: {tables_to_replicate}")
    
    # Create and run orchestrator
    orchestrator = ReplicationOrchestrator(
        source_db_config=source_config,
        rabbitmq_url=settings.rmq_url,
        exchange_name=settings.rmq_exchange,
        channel_name=settings.pg_channel,
        state_store=state_store,
        tables_to_replicate=tables_to_replicate,
        replica_configs=replica_configs,
        enable_http_server=enable_http_server,
        http_port=http_port,
    )
    
    try:
        await orchestrator.run_forever()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Service error: {e}")
        raise
    finally:
        logger.info("Service shutdown complete")


async def run_simple(settings: Settings):
    """
    Run a simple publisher-only mode (backward compatible).
    
    This mode only publishes changes to RabbitMQ without consuming.
    Useful for testing or when you want to handle consumption separately.
    
    Args:
        settings: Application settings
    """
    from axis.db.listener import PostgresListener
    from axis.messaging.publisher import Publisher
    import json
    
    logger.info("Starting Axis in simple publisher mode...")
    
    async def _worker_loop(in_queue: asyncio.Queue, publisher: Publisher):
        while True:
            payload = await in_queue.get()
            try:
                try:
                    body = json.loads(payload)
                except json.JSONDecodeError:
                    body = {"raw": payload}
                except Exception:
                    body = {"raw": payload}
                envelope = {
                    "source": "db-trigger",
                    "data": body,
                    "version": 1,
                }
                await publisher.publish(json.dumps(envelope))
            finally:
                in_queue.task_done()

    q: asyncio.Queue = asyncio.Queue(maxsize=10000)
    
    # Start components
    listener = PostgresListener(settings.pg_dsn, settings.pg_channel, q)
    publisher = Publisher(
        settings.rmq_url, settings.rmq_exchange, settings.rmq_exchange_type
    )

    await publisher.start()
    await listener.start()

    worker = asyncio.create_task(_worker_loop(q, publisher))

    # Supervise until cancelled
    try:
        await worker
    except asyncio.CancelledError:
        pass
    finally:
        await listener.stop()
        await publisher.close()

