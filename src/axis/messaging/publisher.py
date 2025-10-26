import aio_pika
from aio_pika import Message, ExchangeType
import asyncio


class Publisher:
    """
    aio-pika publisher that declares an exchange and publishes messages.
    Uses connect_robust for automatic reconnection.
    """

    def __init__(self, url: str, exchange_name: str, exchange_type: str = "fanout"):
        self.url = url
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self._connection: aio_pika.RobustConnection | None = None
        self._channel: aio_pika.RobustChannel | None = None
        self._exchange: aio_pika.Exchange | None = None

    async def start(self):
        self._connection = await aio_pika.connect_robust(self.url)
        self._channel = await self._connection.channel()
        self._exchange = await self._channel.declare_exchange(
            self.exchange_name, type=self.exchange_type, durable=True
        )

    async def publish(self, payload: str, routing_key: str = ""):
        if not self._exchange:
            raise RuntimeError("Publisher not started. Call start() before publishing.")
        message = Message(payload.encode())
        await self._exchange.publish(message, routing_key=routing_key)

    async def close(self):
        if self._connection:
            await self._connection.close()
