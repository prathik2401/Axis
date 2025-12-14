from abc import ABC, abstractmethod
from typing import Optional
import asyncio
import aio_pika
from aio_pika.abc import (
    AbstractRobustConnection, 
    AbstractChannel, 
    AbstractQueue,
    AbstractIncomingMessage
)
from utils.logging import get_logger

logger = get_logger(__name__)


class BaseConsumer(ABC):
    """
    Abstract base class for RabbitMQ consumers.
    Handles message consumption and processing.
    """
    
    def __init__(
        self,
        rabbitmq_url: str,
        exchange_name: str,
        queue_name: str,
        routing_key: str = "",
        prefetch_count: int = 10
    ):
        self.rabbitmq_url = rabbitmq_url
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.routing_key = routing_key
        self.prefetch_count = prefetch_count
        
        self._connection: Optional[AbstractRobustConnection] = None
        self._channel: Optional[AbstractChannel] = None
        self._queue: Optional[AbstractQueue] = None
        self._running = False
    
    async def start(self) -> None:
        """Connect to RabbitMQ and start consuming."""
        try:
            self._connection = await aio_pika.connect_robust(self.rabbitmq_url)
            self._channel = await self._connection.channel()
            
            # Set QoS (prefetch)
            await self._channel.set_qos(prefetch_count=self.prefetch_count)
            
            # Declare exchange
            exchange = await self._channel.declare_exchange(
                self.exchange_name,
                type=aio_pika.ExchangeType.FANOUT,
                durable=True
            )
            
            # Declare queue
            self._queue = await self._channel.declare_queue(
                self.queue_name,
                durable=True
            )
            
            # Bind queue to exchange
            await self._queue.bind(exchange, routing_key=self.routing_key)
            
            self._running = True
            logger.info(f"Consumer started: {self.queue_name}")
            
            # Start consuming
            await self._queue.consume(self._on_message)
            
        except Exception as e:
            logger.error(f"Failed to start consumer: {e}")
            raise
    
    async def stop(self) -> None:
        """Stop consuming and close connections."""
        self._running = False
        
        if self._connection:
            await self._connection.close()
        
        logger.info(f"Consumer stopped: {self.queue_name}")
    
    async def _on_message(self, message: AbstractIncomingMessage) -> None:
        """
        Internal message handler that wraps process_message.
        
        Args:
            message: Incoming RabbitMQ message
        """
        async with message.process():
            try:
                await self.process_message(message.body.decode())
                logger.debug(f"Processed message: {message.message_id}")
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                # Message will be requeued by RabbitMQ
                raise
    
    @abstractmethod
    async def process_message(self, payload: str) -> None:
        """
        Process a single message. Must be implemented by subclasses.
        
        Args:
            payload: Message payload as string (usually JSON)
        """
        pass
    
    @property
    def is_running(self) -> bool:
        """Check if consumer is running."""
        return self._running