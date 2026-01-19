import asyncio
import asyncpg


async def main():
    conn = await asyncpg.connect(
        "postgresql://postgres:password@192.168.29.5:5432/source_db"
    )

    # Check existing users
    users = await conn.fetch("SELECT id, email FROM users")
    print(f"Current users: {len(users)}")
    for u in users:
        print(f'  - ID: {u["id"]}, Email: {u["email"]}')

    # Insert a simple user and check
    await conn.execute("DELETE FROM orders WHERE user_id IS NOT NULL")
    await conn.execute("DELETE FROM users WHERE email LIKE '%test%'")

    result = await conn.fetchrow(
        "INSERT INTO users (email, name) VALUES ('test@example.com', 'Test User') RETURNING id, email"
    )
    print(f'\nInserted user: ID={result["id"]}, Email={result["email"]}')

    # Insert order for this user
    await conn.execute(
        f"INSERT INTO orders (user_id, product_name, quantity, price) VALUES ({result['id']}, 'Test Product', 1, 99.99)"
    )
    print("Inserted order successfully!")

    await conn.close()


asyncio.run(main())
