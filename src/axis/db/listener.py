import asyncio
import asyncpg
from typing import Awaitable, Callable, Optional


class PostgresListener:
    """
    Minimal async listener that LISTENs on a channel and funnels payloads
    into an asyncio.Queue (or calls a callback).
    """

    def __init__(self, dsn: str, channel: str, output_queue: asyncio.Queue):
        self._dsn = dsn
        self._channel = channel
        self._queue = output_queue
        self._conn: Optional[asyncpg.Connection] = None
        self._running = False

    async def start(self):
        self._conn = await asyncpg.connect(self._dsn)

        # asyncpg callback signature: (connection, pid, channel, payload)
        def _cb(conn, pid, ch, payload):
            asyncio.get_event_loop().create_task(self._queue.put(payload))

        await self._conn.add_listener(self._channel, _cb)
        self._running = True

    async def stop(self):
        if not self._conn:
            return

        try:
            await self._conn.remove_listener(self._channel, lambda *a: None)
        except Exception:
            pass
        await self._conn.close()
        self._running = False
