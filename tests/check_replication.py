import asyncio
import asyncpg


async def main():
    # Check source
    source_conn = await asyncpg.connect(
        "postgresql://postgres:password@192.168.29.5:5432/source_db"
    )
    source_users = await source_conn.fetch("SELECT id, email FROM users ORDER BY id")
    source_orders = await source_conn.fetch(
        "SELECT id, user_id, product_name FROM orders ORDER BY id"
    )
    await source_conn.close()

    # Check replica
    replica_conn = await asyncpg.connect(
        "postgresql://postgres:password@192.168.29.5:5433/replica_db"
    )
    replica_users = await replica_conn.fetch("SELECT id, email FROM users ORDER BY id")
    replica_orders = await replica_conn.fetch(
        "SELECT id, user_id, product_name FROM orders ORDER BY id"
    )
    await replica_conn.close()

    print("=" * 60)
    print("SOURCE DATABASE (192.168.29.5:5432/source_db)")
    print("=" * 60)
    print(f"\nUsers ({len(source_users)}):")
    for u in source_users[:5]:
        print(f"  - ID: {u['id']:3d}, Email: {u['email']}")
    if len(source_users) > 5:
        print(f"  ... and {len(source_users) - 5} more")

    print(f"\nOrders ({len(source_orders)}):")
    for o in source_orders[:5]:
        print(
            f"  - ID: {o['id']:3d}, User: {o['user_id']:3d}, Product: {o['product_name']}"
        )
    if len(source_orders) > 5:
        print(f"  ... and {len(source_orders) - 5} more")

    print("\n" + "=" * 60)
    print("REPLICA DATABASE (192.168.29.5:5433/replica_db)")
    print("=" * 60)
    print(f"\nUsers ({len(replica_users)}):")
    for u in replica_users[:5]:
        print(f"  - ID: {u['id']:3d}, Email: {u['email']}")
    if len(replica_users) > 5:
        print(f"  ... and {len(replica_users) - 5} more")

    print(f"\nOrders ({len(replica_orders)}):")
    for o in replica_orders[:5]:
        print(
            f"  - ID: {o['id']:3d}, User: {o['user_id']:3d}, Product: {o['product_name']}"
        )
    if len(replica_orders) > 5:
        print(f"  ... and {len(replica_orders) - 5} more")

    print("\n" + "=" * 60)
    print("REPLICATION STATUS")
    print("=" * 60)
    users_diff = len(source_users) - len(replica_users)
    orders_diff = len(source_orders) - len(replica_orders)

    if users_diff == 0 and orders_diff == 0:
        print("✓ All data replicated successfully!")
    else:
        print(f"Users difference: {users_diff}")
        print(f"Orders difference: {orders_diff}")


asyncio.run(main())
