import json
import asyncio
from typing import Optional, List, Tuple
from datetime import datetime
import hashlib

from .base import BaseConsumer
from axis.plugins.base import ChangeEvent, DatabaseConfig
from axis.plugins.postgres import PostgresPlugin
from axis.replication.state_store import StateStore, EventTracker, ReplicationCheckpoint
from utils.logging import get_logger

logger = get_logger(__name__)


class PostgresReplayConsumer(BaseConsumer):
    """
    RabbitMQ consumer that replays database changes to PostgreSQL replicas.
    
    Features:
    - Idempotency checks (prevents duplicate replays)
    - Automatic retries with exponential backoff
    - Batch processing for performance
    - State tracking and checkpointing
    """
    
    def __init__(
        self,
        rabbitmq_url: str,
        exchange_name: str,
        queue_name: str,
        replica_db_config: DatabaseConfig,
        state_store: StateStore,
        routing_key: str = "",
        prefetch_count: int = 10,
        batch_size: int = 10,
        batch_timeout_seconds: float = 5.0,
    ):
        """
        Initialize PostgreSQL replay consumer.
        
        Args:
            rabbitmq_url: RabbitMQ connection URL
            exchange_name: RabbitMQ exchange name
            queue_name: RabbitMQ queue name
            replica_db_config: Configuration for replica database
            state_store: State store for tracking replication
            routing_key: RabbitMQ routing key
            prefetch_count: Number of messages to prefetch
            batch_size: Number of events to batch before applying
            batch_timeout_seconds: Max time to wait before applying partial batch
        """
        super().__init__(
            rabbitmq_url=rabbitmq_url,
            exchange_name=exchange_name,
            queue_name=queue_name,
            routing_key=routing_key,
            prefetch_count=prefetch_count,
        )
        
        self.replica_plugin = PostgresPlugin(replica_db_config)
        self.state_store = state_store
        self.batch_size = batch_size
        self.batch_timeout_seconds = batch_timeout_seconds
        
        self._batch: List[Tuple[ChangeEvent, str]] = []
        self._batch_lock = asyncio.Lock()
        self._batch_timer: Optional[asyncio.Task] = None
        
        # Metrics
        self._processed_count = 0
        self._failed_count = 0
        self._batch_count = 0
    
    async def start(self) -> None:
        """Start the consumer and connect to replica database."""
        # Connect to replica database
        await self.replica_plugin.connect()
        logger.info("Connected to replica database")
        
        # Start RabbitMQ consumer
        await super().start()
        
        # Start batch timer
        self._batch_timer = asyncio.create_task(self._batch_timeout_handler())
    
    async def stop(self) -> None:
        """Stop the consumer and cleanup."""
        # Cancel batch timer
        if self._batch_timer:
            self._batch_timer.cancel()
            try:
                await self._batch_timer
            except asyncio.CancelledError:
                pass
        
        # Flush any remaining batched events
        await self._flush_batch()
        
        # Disconnect from replica
        await self.replica_plugin.disconnect()
        
        # Stop RabbitMQ consumer
        await super().stop()
        
        logger.info(
            f"Consumer stopped. Processed: {self._processed_count}, "
            f"Failed: {self._failed_count}, Batches: {self._batch_count}"
        )
    
    async def process_message(self, payload: str) -> None:
        """
        Process a single message from RabbitMQ.
        
        Args:
            payload: JSON-encoded message payload
        """
        try:
            # Parse message envelope
            envelope = json.loads(payload)
            data = envelope.get('data', {})
            
            # Extract change event
            change_event = ChangeEvent(
                table=data.get('table'),
                schema=data.get('schema'),
                operation=data.get('operation'),
                payload=data.get('payload', {}),
                old_values=data.get('old_values'),
                timestamp=data.get('timestamp', datetime.now().isoformat()),
            )
            
            # Generate event ID for idempotency
            event_id = self._generate_event_id(change_event)
            
            # Check if already processed (idempotency)
            existing_event = await self.state_store.get_event_status(event_id)
            if existing_event and existing_event.status == 'processed':
                logger.debug(f"Skipping already processed event: {event_id}")
                return
            
            # Track event
            event_tracker = EventTracker(
                event_id=event_id,
                table=change_event.table,
                operation=change_event.operation,
                timestamp=change_event.timestamp,
                status='pending',
            )
            await self.state_store.track_event(event_tracker)
            
            # Add to batch
            await self._add_to_batch(change_event, event_id)
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode message: {e}")
            raise
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            raise
    
    async def _add_to_batch(self, event: ChangeEvent, event_id: str) -> None:
        """
        Add event to batch and flush if batch is full.
        
        Args:
            event: Change event to add
            event_id: Event identifier
        """
        async with self._batch_lock:
            self._batch.append((event, event_id))
            
            if len(self._batch) >= self.batch_size:
                await self._flush_batch()
    
    async def _flush_batch(self) -> None:
        """Apply all events in current batch."""
        async with self._batch_lock:
            if not self._batch:
                return
            
            batch_to_process = self._batch.copy()
            self._batch.clear()
        
        logger.info(f"Flushing batch of {len(batch_to_process)} events")
        
        try:
            # Apply batch in transaction
            events = [e[0] for e in batch_to_process]
            event_ids = [e[1] for e in batch_to_process]
            
            success_count = await self.replica_plugin.execute_batch(events)
            
            # Mark events as processed
            for event_id in event_ids[:success_count]:
                await self.state_store.mark_event_processed(event_id)
                self._processed_count += 1
            
            # Mark failed events
            for event_id in event_ids[success_count:]:
                await self.state_store.mark_event_failed(
                    event_id, 
                    "Batch processing failed",
                    retry=True
                )
                self._failed_count += 1
            
            self._batch_count += 1
            
            # Update checkpoint
            if success_count > 0:
                checkpoint = ReplicationCheckpoint(
                    sequence_number=self._processed_count,
                    timestamp=datetime.now().isoformat(),
                    source_db="primary",
                    target_db=self.replica_plugin.config.dsn,
                    tables_processed=success_count,
                    status='active',
                )
                await self.state_store.save_checkpoint(
                    f"replica_{self.queue_name}",
                    checkpoint
                )
            
            logger.info(
                f"Batch processed: {success_count}/{len(batch_to_process)} successful"
            )
            
        except Exception as e:
            logger.error(f"Batch processing error: {e}")
            
            # Mark all events as failed
            for _, event_id in batch_to_process:
                await self.state_store.mark_event_failed(
                    event_id,
                    str(e),
                    retry=True
                )
                self._failed_count += 1
    
    async def _batch_timeout_handler(self) -> None:
        """Periodically flush batch even if not full."""
        while self._running:
            try:
                await asyncio.sleep(self.batch_timeout_seconds)
                
                async with self._batch_lock:
                    if self._batch:
                        logger.debug("Flushing batch due to timeout")
                
                await self._flush_batch()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Batch timeout handler error: {e}")
    
    @staticmethod
    def _generate_event_id(event: ChangeEvent) -> str:
        """
        Generate deterministic event ID for idempotency.
        
        Args:
            event: Change event
            
        Returns:
            Unique event ID
        """
        # Create hash from event components
        event_str = f"{event.table}:{event.operation}:{event.timestamp}:{json.dumps(event.payload, sort_keys=True)}"
        return hashlib.sha256(event_str.encode()).hexdigest()[:16]
    
    async def get_metrics(self) -> dict:
        """Get consumer metrics."""
        return {
            'queue_name': self.queue_name,
            'processed_count': self._processed_count,
            'failed_count': self._failed_count,
            'batch_count': self._batch_count,
            'current_batch_size': len(self._batch),
            'is_running': self.is_running,
            'replica_connected': self.replica_plugin.is_connected,
        }
