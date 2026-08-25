#!/usr/bin/env python3
"""E2E test: cluster of 2 source DBs, each with its own replica.

Requires two Axis orchestrator processes (unique PG_CHANNEL / RMQ_EXCHANGE
per source) and matching schemas on sources and replicas.

Environment overrides (optional):
  SOURCE_DB1, SOURCE_DB2, REPLICA_DB1, REPLICA_DB2, REPLICATION_DELAY
"""

import asyncio
import asyncpg
import os
import sys
import time

PAIRS = [
    {
        "name": "pair1",
        "source": os.getenv(
            "SOURCE_DB1",
            "postgresql://postgres:postgres@127.0.0.1:5432/source_db1",
        ),
        "replica": os.getenv(
            "REPLICA_DB1",
            "postgresql://postgres:postgres@127.0.0.1:5432/replica_db1",
        ),
        "email_prefix": "p1",
    },
    {
        "name": "pair2",
        "source": os.getenv(
            "SOURCE_DB2",
            "postgresql://postgres:postgres@127.0.0.1:5432/source_db2",
        ),
        "replica": os.getenv(
            "REPLICA_DB2",
            "postgresql://postgres:postgres@127.0.0.1:5432/replica_db2",
        ),
        "email_prefix": "p2",
    },
]

# Consumer batch timeout is hardcoded ~5s; wait beyond that.
REPLICATION_DELAY = int(os.getenv("REPLICATION_DELAY", "8"))


async def count_rows(dsn: str, table: str) -> int:
    conn = await asyncpg.connect(dsn)
    try:
        return await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
    finally:
        await conn.close()


async def get_name(dsn: str, email: str):
    conn = await asyncpg.connect(dsn)
    try:
        return await conn.fetchval("SELECT name FROM users WHERE email = $1", email)
    finally:
        await conn.close()


async def insert_user(dsn: str, name: str, email: str):
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(
            "INSERT INTO users (name, email) VALUES ($1, $2)", name, email
        )
        print(f"  ✓ INSERT {email} into source")
    finally:
        await conn.close()


async def update_user(dsn: str, email: str, new_name: str):
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(
            "UPDATE users SET name = $1 WHERE email = $2", new_name, email
        )
        print(f"  ✓ UPDATE {email} -> {new_name}")
    finally:
        await conn.close()


async def delete_user(dsn: str, email: str):
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute("DELETE FROM users WHERE email = $1", email)
        print(f"  ✓ DELETE {email}")
    finally:
        await conn.close()


async def verify_counts(pair, label) -> bool:
    src = await count_rows(pair["source"], "users")
    rep = await count_rows(pair["replica"], "users")
    ok = src == rep
    status = "PASS" if ok else "FAIL"
    print(f"  [{status}] {pair['name']} {label}: source={src} replica={rep}")
    return ok


async def verify_name(pair, email, expected) -> bool:
    src = await get_name(pair["source"], email)
    rep = await get_name(pair["replica"], email)
    ok = src == expected and rep == expected
    status = "PASS" if ok else "FAIL"
    print(
        f"  [{status}] {pair['name']} UPDATE check: "
        f"source={src!r} replica={rep!r} expected={expected!r}"
    )
    return ok


async def verify_isolation(pair_a, pair_b, email_a) -> bool:
    """Ensure pair_b replica does not contain pair_a's inserted email."""
    conn = await asyncpg.connect(pair_b["replica"])
    try:
        found = await conn.fetchval(
            "SELECT COUNT(*) FROM users WHERE email = $1", email_a
        )
    finally:
        await conn.close()
    ok = found == 0
    status = "PASS" if ok else "FAIL"
    print(
        f"  [{status}] isolation: {email_a} absent from {pair_b['name']} replica "
        f"(count={found})"
    )
    return ok


async def main():
    print("=" * 70)
    print(" E2E: 2-DB cluster + replicas")
    print("=" * 70)

    passed = 0
    failed = 0

    # --- INSERT on both sources ---
    print("\nTEST 1: INSERT on both sources")
    emails = {}
    for pair in PAIRS:
        email = f"{pair['email_prefix']}-insert-{int(time.time())}@example.com"
        emails[pair["name"]] = email
        await insert_user(pair["source"], f"User {pair['name']}", email)

    print(f"Waiting {REPLICATION_DELAY}s for replication...")
    await asyncio.sleep(REPLICATION_DELAY)

    for pair in PAIRS:
        if await verify_counts(pair, "after INSERT"):
            passed += 1
        else:
            failed += 1

    # Isolation: pair1 insert should not land on pair2 replica and vice versa
    print("\nTEST 2: Cross-pair isolation")
    if await verify_isolation(PAIRS[0], PAIRS[1], emails["pair1"]):
        passed += 1
    else:
        failed += 1
    if await verify_isolation(PAIRS[1], PAIRS[0], emails["pair2"]):
        passed += 1
    else:
        failed += 1

    # --- UPDATE ---
    print("\nTEST 3: UPDATE on both sources")
    for pair in PAIRS:
        await update_user(
            pair["source"], emails[pair["name"]], f"Updated {pair['name']}"
        )

    print(f"Waiting {REPLICATION_DELAY}s for replication...")
    await asyncio.sleep(REPLICATION_DELAY)

    for pair in PAIRS:
        if await verify_name(pair, emails[pair["name"]], f"Updated {pair['name']}"):
            passed += 1
        else:
            failed += 1

    # --- DELETE ---
    print("\nTEST 4: DELETE on both sources")
    for pair in PAIRS:
        await delete_user(pair["source"], emails[pair["name"]])

    print(f"Waiting {REPLICATION_DELAY}s for replication...")
    await asyncio.sleep(REPLICATION_DELAY)

    for pair in PAIRS:
        if await verify_counts(pair, "after DELETE"):
            passed += 1
        else:
            failed += 1

    print("\n" + "=" * 70)
    print(f" SUMMARY: {passed} passed, {failed} failed")
    print("=" * 70)
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    asyncio.run(main())
