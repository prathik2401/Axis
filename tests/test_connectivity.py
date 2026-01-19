"""
Test database and RabbitMQ connectivity before running Axis.
"""

import asyncio
import sys
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()


async def test_postgres_connection(dsn: str, name: str):
    """Test PostgreSQL connection."""
    try:
        import asyncpg

        print(f"Testing {name}...")
        conn = await asyncpg.connect(dsn)
        version = await conn.fetchval("SELECT version()")
        await conn.close()
        print(f"✓ {name} connected successfully!")
        print(f"  Version: {version.split(',')[0]}")
        return True
    except Exception as e:
        print(f"✗ {name} connection failed: {e}")
        return False


async def test_rabbitmq_connection(url: str):
    """Test RabbitMQ connection."""
    try:
        import aio_pika

        print(f"Testing RabbitMQ...")
        connection = await aio_pika.connect_robust(url)
        channel = await connection.channel()
        await channel.close()
        await connection.close()
        print(f"✓ RabbitMQ connected successfully!")
        return True
    except Exception as e:
        print(f"✗ RabbitMQ connection failed: {e}")
        return False


async def check_source_tables(dsn: str):
    """Check if test tables exist in source database."""
    try:
        import asyncpg

        print(f"\nChecking source database tables...")
        conn = await asyncpg.connect(dsn)

        tables = await conn.fetch(
            """
            SELECT table_name, 
                   (SELECT COUNT(*) FROM information_schema.columns 
                    WHERE table_name = t.table_name) as column_count
            FROM information_schema.tables t
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        )

        if tables:
            print(f"✓ Found {len(tables)} tables:")
            for table in tables:
                # Get row count
                count = await conn.fetchval(
                    f"SELECT COUNT(*) FROM {table['table_name']}"
                )
                print(
                    f"  - {table['table_name']}: {table['column_count']} columns, {count} rows"
                )
        else:
            print("⚠ No tables found in source database!")
            print(
                "  Run: psql -h 192.168.29.5 -U postgres -d source_db -f tests/setup_test_schema.sql"
            )

        await conn.close()
        return len(tables) > 0
    except Exception as e:
        print(f"✗ Failed to check tables: {e}")
        return False


async def main():
    """Run all connectivity tests."""
    print("=" * 60)
    print("AXIS Connectivity Test")
    print("=" * 60)

    # Get configuration from environment
    source_dsn = os.getenv(
        "PG_DSN", "postgresql://postgres:password@192.168.29.5:5432/source_db"
    )
    replica_dsn = "postgresql://postgres:password@192.168.29.5:5433/replica_db"
    rabbitmq_url = os.getenv("RMQ_URL", "amqp://guest:guest@192.168.29.5:5672/")

    print(f"\nConfiguration:")
    print(
        f"  Source DB:  {source_dsn.split('@')[1] if '@' in source_dsn else source_dsn}"
    )
    print(
        f"  Replica DB: {replica_dsn.split('@')[1] if '@' in replica_dsn else replica_dsn}"
    )
    print(
        f"  RabbitMQ:   {rabbitmq_url.split('@')[1] if '@' in rabbitmq_url else rabbitmq_url}"
    )
    print()

    # Run tests
    results = []

    # Test source database
    results.append(await test_postgres_connection(source_dsn, "Source Database"))

    # Test replica database
    results.append(await test_postgres_connection(replica_dsn, "Replica Database"))

    # Test RabbitMQ
    results.append(await test_rabbitmq_connection(rabbitmq_url))

    # Check tables
    if results[0]:  # Only if source DB connected
        results.append(await check_source_tables(source_dsn))

    # Summary
    print("\n" + "=" * 60)
    if all(results):
        print("✓ All connectivity tests passed!")
        print("=" * 60)
        print("\nYou're ready to start Axis!")
        print("\nRun one of these commands:")
        print("  1. Simple mode:        python src/cli.py --simple")
        print(
            '  2. Full replication:   python src/cli.py --replicas "postgresql://postgres:password@192.168.29.5:5433/replica_db"'
        )
        return 0
    else:
        print("✗ Some tests failed. Please fix the issues above.")
        print("=" * 60)
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
