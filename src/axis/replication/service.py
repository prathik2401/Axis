import json
import asyncio
from config.settings import Settings
from axis.db.listener import PostgresListener
from axis.messaging.publisher import Publisher


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


async def run(settings: Settings):
    q: asyncio.Queue = asyncio.Queue(maxsize=10000)
    # start components
    listener = PostgresListener(settings.pg_dsn, settings.pg_channel, q)
    publisher = Publisher(
        settings.rmq_url, settings.rmq_exchange, settings.rmq_exchange_type
    )

    await publisher.start()
    await listener.start()

    worker = asyncio.create_task(_worker_loop(q, publisher))

    # supervise until cancelled
    try:
        await worker
    except asyncio.CancelledError:
        pass
    finally:
        await listener.stop()
        await publisher.close()
