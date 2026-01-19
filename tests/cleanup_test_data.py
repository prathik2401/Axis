"""Clean up test users from previous test runs."""

import asyncio
import asyncpg

SOURCE_DSN = "postgresql://postgres:password@192.168.29.5:5432/source_db"


async def cleanup():
    """Remove test users."""
    conn = await asyncpg.connect(SOURCE_DSN)
    try:
        result = await conn.execute(
            "DELETE FROM users WHERE email LIKE 'test%@example.com'"
        )
        print(f"Cleaned up test users: {result}")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(cleanup())
