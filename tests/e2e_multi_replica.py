#!/usr/bin/env python3
"""E2E test: one source DB replicated to two replicas.

Start Axis with two --replicas DSNs pointing at replica_db1 and replica_db2.

Environment overrides (optional):
  SOURCE_DB, REPLICA_DB1, REPLICA_DB2, REPLICATION_DELAY
"""

import asyncio
import asyncpg
import os
import sys
import time

SOURCE = os.getenv(
    "SOURCE_DB", "postgresql://postgres:postgres@127.0.0.1:5432/source_db1"
)
REPLICAS = [
    os.getenv(
        "REPLICA_DB1", "postgresql://postgres:postgres@127.0.0.1:5432/replica_db1"
    ),
    os.getenv(
        "REPLICA_DB2", "postgresql://postgres:postgres@127.0.0.1:5432/replica_db2"
    ),
]
REPLICATION_DELAY = int(os.getenv("REPLICATION_DELAY", "8"))


async def count_users(dsn: str) -> int:
    conn = await asyncpg.connect(dsn)
    try:
        return await conn.fetchval("SELECT COUNT(*) FROM users")
    finally:
        await conn.close()


async def get_name(dsn: str, email: str):
    conn = await asyncpg.connect(dsn)
    try:
        return await conn.fetchval("SELECT name FROM users WHERE email = $1", email)
    finally:
        await conn.close()


async def main():
    print("=" * 70)
    print(" E2E: 1 source -> 2 replicas")
    print("=" * 70)
    passed = failed = 0
    email = f"multi-{int(time.time())}@example.com"

    conn = await asyncpg.connect(SOURCE)
    await conn.execute(
        "INSERT INTO users (name, email) VALUES ($1, $2)", "Multi User", email
    )
    await conn.close()
    print(f"✓ INSERT {email}")
    await asyncio.sleep(REPLICATION_DELAY)

    src = await count_users(SOURCE)
    for i, r in enumerate(REPLICAS):
        rep = await count_users(r)
        ok = src == rep == 4
        print(f"  [{'PASS' if ok else 'FAIL'}] after INSERT source={src} replica{i}={rep}")
        passed += ok
        failed += not ok

    conn = await asyncpg.connect(SOURCE)
    await conn.execute("UPDATE users SET name = $1 WHERE email = $2", "Multi Updated", email)
    await conn.close()
    print("✓ UPDATE")
    await asyncio.sleep(REPLICATION_DELAY)

    for i, r in enumerate(REPLICAS):
        name = await get_name(r, email)
        ok = name == "Multi Updated"
        print(f"  [{'PASS' if ok else 'FAIL'}] after UPDATE replica{i}={name!r}")
        passed += ok
        failed += not ok

    conn = await asyncpg.connect(SOURCE)
    await conn.execute("DELETE FROM users WHERE email = $1", email)
    await conn.close()
    print("✓ DELETE")
    await asyncio.sleep(REPLICATION_DELAY)

    src = await count_users(SOURCE)
    for i, r in enumerate(REPLICAS):
        rep = await count_users(r)
        ok = src == rep == 3
        print(f"  [{'PASS' if ok else 'FAIL'}] after DELETE source={src} replica{i}={rep}")
        passed += ok
        failed += not ok

    print(f"\nSUMMARY: {passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    asyncio.run(main())
