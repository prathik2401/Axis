"""
Quick script to insert test data into source database.
"""

import asyncio
import asyncpg


async def insert_test_data():
    """Insert test data into source database."""
    dsn = "postgresql://postgres:password@192.168.29.5:5432/source_db"

    print("Connecting to source database...")
    conn = await asyncpg.connect(dsn)

    print("Inserting test data...")

    # Insert users
    await conn.execute(
        """
        INSERT INTO users (email, name, phone) VALUES
            ('john@example.com', 'John Doe', '555-1234'),
            ('jane@example.com', 'Jane Smith', '555-5678'),
            ('bob@example.com', 'Bob Johnson', '555-9012')
        ON CONFLICT (email) DO NOTHING
    """
    )

    # Insert products
    await conn.execute(
        """
        INSERT INTO products (name, price, stock) VALUES
            ('Laptop', 1299.99, 50),
            ('Mouse', 29.99, 200),
            ('Keyboard', 149.99, 100)
    """
    )

    # Insert orders
    await conn.execute(
        """
        INSERT INTO orders (user_id, product_name, quantity, price) VALUES
            (1, 'Laptop', 1, 1299.99),
            (2, 'Mouse', 2, 29.99),
            (3, 'Keyboard', 1, 149.99)
    """
    )

    # Verify
    users_count = await conn.fetchval("SELECT COUNT(*) FROM users")
    products_count = await conn.fetchval("SELECT COUNT(*) FROM products")
    orders_count = await conn.fetchval("SELECT COUNT(*) FROM orders")

    print(f"✓ Inserted data:")
    print(f"  Users: {users_count}")
    print(f"  Products: {products_count}")
    print(f"  Orders: {orders_count}")

    await conn.close()
    print("\n✓ Test data ready!")


if __name__ == "__main__":
    asyncio.run(insert_test_data())
