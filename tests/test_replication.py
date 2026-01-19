"""
Test script to verify real-time replication from source to replica database.
This script should be run while Axis is running in a separate terminal.
"""

import asyncio
import asyncpg
import sys

SOURCE_DSN = "postgresql://postgres:password@192.168.29.5:5432/source_db"
REPLICA_DSN = "postgresql://postgres:password@192.168.29.5:5433/replica_db"

# Wait time for replication to propagate
REPLICATION_DELAY = 3


async def count_rows(dsn: str, table: str) -> int:
    """Count rows in a table."""
    conn = await asyncpg.connect(dsn)
    try:
        count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
        return count
    finally:
        await conn.close()


async def insert_test_user(name: str, email: str):
    """Insert a test user into source database."""
    conn = await asyncpg.connect(SOURCE_DSN)
    try:
        await conn.execute(
            "INSERT INTO users (name, email) VALUES ($1, $2)", name, email
        )
        print(f"✓ Inserted user: {name} ({email})")
    finally:
        await conn.close()


async def update_test_user(email: str, new_name: str):
    """Update a user in source database."""
    conn = await asyncpg.connect(SOURCE_DSN)
    try:
        await conn.execute(
            "UPDATE users SET name = $1 WHERE email = $2", new_name, email
        )
        print(f"✓ Updated user {email} -> {new_name}")
    finally:
        await conn.close()


async def delete_test_user(email: str):
    """Delete a user from source database."""
    conn = await asyncpg.connect(SOURCE_DSN)
    try:
        await conn.execute("DELETE FROM users WHERE email = $1", email)
        print(f"✓ Deleted user: {email}")
    finally:
        await conn.close()


async def verify_replication():
    """Verify data is replicated."""
    print("\n--- Verifying Replication ---")

    # Count rows in both databases
    source_users = await count_rows(SOURCE_DSN, "users")
    replica_users = await count_rows(REPLICA_DSN, "users")

    print(f"  Source DB users: {source_users}")
    print(f"  Replica DB users: {replica_users}")

    if source_users == replica_users:
        print("  ✅ PASS - Row counts match")
        return True
    else:
        print(f"  ❌ FAIL - Mismatch! Source={source_users}, Replica={replica_users}")
        return False


async def get_user_name(dsn: str, email: str) -> str:
    """Get user name by email."""
    conn = await asyncpg.connect(dsn)
    try:
        name = await conn.fetchval("SELECT name FROM users WHERE email = $1", email)
        return name if name else ""
    finally:
        await conn.close()


async def main():
    """Run replication tests."""
    print("=" * 70)
    print(" AXIS REPLICATION TEST")
    print("=" * 70)
    print("\nMake sure Axis is running in a separate terminal before continuing!")
    print("Press Ctrl+C to cancel, or wait 5 seconds to start...\n")

    try:
        await asyncio.sleep(5)
    except KeyboardInterrupt:
        print("\nTest cancelled by user")
        return

    tests_passed = 0
    tests_failed = 0

    # Test 1: INSERT
    print("\n" + "=" * 70)
    print("TEST 1: INSERT Operation")
    print("=" * 70)
    await insert_test_user("Test User 1", "test1@example.com")
    print(f"Waiting {REPLICATION_DELAY} seconds for replication...")
    await asyncio.sleep(REPLICATION_DELAY)
    if await verify_replication():
        tests_passed += 1
    else:
        tests_failed += 1

    # Test 2: Another INSERT
    print("\n" + "=" * 70)
    print("TEST 2: Another INSERT")
    print("=" * 70)
    await insert_test_user("Test User 2", "test2@example.com")
    print(f"Waiting {REPLICATION_DELAY} seconds for replication...")
    await asyncio.sleep(REPLICATION_DELAY)
    if await verify_replication():
        tests_passed += 1
    else:
        tests_failed += 1

    # Test 3: UPDATE
    print("\n" + "=" * 70)
    print("TEST 3: UPDATE Operation")
    print("=" * 70)
    await update_test_user("test1@example.com", "Updated User 1")
    print(f"Waiting {REPLICATION_DELAY} seconds for replication...")
    await asyncio.sleep(REPLICATION_DELAY)

    # Verify the update propagated
    source_name = await get_user_name(SOURCE_DSN, "test1@example.com")
    replica_name = await get_user_name(REPLICA_DSN, "test1@example.com")
    print(f"  Source name: {source_name}")
    print(f"  Replica name: {replica_name}")
    if source_name == replica_name == "Updated User 1":
        print("  ✅ PASS - UPDATE replicated correctly")
        tests_passed += 1
    else:
        print("  ❌ FAIL - UPDATE not replicated")
        tests_failed += 1

    # Test 4: DELETE
    print("\n" + "=" * 70)
    print("TEST 4: DELETE Operation")
    print("=" * 70)
    await delete_test_user("test2@example.com")
    print(f"Waiting {REPLICATION_DELAY} seconds for replication...")
    await asyncio.sleep(REPLICATION_DELAY)
    if await verify_replication():
        tests_passed += 1
    else:
        tests_failed += 1

    # Summary
    print("\n" + "=" * 70)
    print(" TEST SUMMARY")
    print("=" * 70)
    print(f"  Tests Passed: {tests_passed}/4")
    print(f"  Tests Failed: {tests_failed}/4")
    print("=" * 70)

    if tests_failed == 0:
        print("\n🎉 ALL TESTS PASSED! Replication is working correctly.\n")
    else:
        print(f"\n⚠️  {tests_failed} test(s) failed. Check Axis logs for errors.\n")

    sys.exit(0 if tests_failed == 0 else 1)


if __name__ == "__main__":
    asyncio.run(main())
