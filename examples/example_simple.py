"""
Example: Simple Publisher-Only Setup

This is the simplest way to use Axis - just publish changes to RabbitMQ.
"""
import asyncio
from dotenv import load_dotenv
from config.settings import Settings
from axis.replication.service import run_simple


async def main():
    load_dotenv()
    settings = Settings()
    
    print("Starting Axis in simple publisher mode...")
    print(f"Listening on: {settings.pg_channel}")
    print(f"Publishing to: {settings.rmq_exchange}")
    
    await run_simple(settings)


if __name__ == "__main__":
    asyncio.run(main())
