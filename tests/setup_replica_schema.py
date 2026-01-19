"""
Setup replica database schema to match source database.
This creates the same tables in the replica without data.
"""

import asyncio
import asyncpg

SOURCE_DSN = "postgresql://postgres:password@192.168.29.5:5432/source_db"
REPLICA_DSN = "postgresql://postgres:password@192.168.29.5:5433/replica_db"


async def get_table_definitions(source_conn, tables):
    """Get CREATE TABLE statements from source database."""
    definitions = {}

    for table in tables:
        # Get table definition
        query = """
        SELECT 
            'CREATE TABLE ' || quote_ident(table_name) || ' (' ||
            string_agg(
                quote_ident(column_name) || ' ' || 
                column_type ||
                CASE WHEN is_nullable = 'NO' THEN ' NOT NULL' ELSE '' END,
                ', '
            ) || ');' as create_stmt
        FROM (
            SELECT 
                table_name,
                column_name,
                CASE 
                    WHEN data_type = 'character varying' THEN 'VARCHAR(' || character_maximum_length || ')'
                    WHEN data_type = 'integer' THEN 'INTEGER'
                    WHEN data_type = 'timestamp without time zone' THEN 'TIMESTAMP'
                    WHEN data_type = 'numeric' THEN 'NUMERIC(' || numeric_precision || ',' || numeric_scale || ')'
                    ELSE UPPER(data_type)
                END as column_type,
                is_nullable,
                ordinal_position
            FROM information_schema.columns
            WHERE table_name = $1
            ORDER BY ordinal_position
        ) cols
        GROUP BY table_name;
        """

        create_stmt = await source_conn.fetchval(query, table)

        # Get primary key
        pk_query = """
        SELECT string_agg(quote_ident(column_name), ', ')
        FROM information_schema.key_column_usage
        WHERE table_name = $1 
        AND constraint_name LIKE '%_pkey'
        GROUP BY table_name;
        """
        pk_cols = await source_conn.fetchval(pk_query, table)

        # Get unique constraints
        unique_query = """
        SELECT string_agg(quote_ident(column_name), ', ')
        FROM information_schema.key_column_usage
        WHERE table_name = $1 
        AND constraint_name LIKE '%_key'
        AND constraint_name NOT LIKE '%_pkey'
        GROUP BY constraint_name;
        """
        unique_cols = await source_conn.fetchval(unique_query, table)

        definitions[table] = {
            "create": create_stmt,
            "primary_key": pk_cols,
            "unique": unique_cols,
        }

    return definitions


async def setup_replica():
    """Setup replica database schema."""
    print("=" * 70)
    print(" SETTING UP REPLICA DATABASE SCHEMA")
    print("=" * 70)
    print()

    # Connect to both databases
    print("Connecting to databases...")
    source_conn = await asyncpg.connect(SOURCE_DSN)
    replica_conn = await asyncpg.connect(REPLICA_DSN)

    try:
        # Get list of tables from source
        tables = await source_conn.fetch(
            """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        )

        table_names = [row["table_name"] for row in tables]
        print(f"Found {len(table_names)} tables in source: {', '.join(table_names)}")
        print()

        # Get table definitions
        print("Fetching table definitions...")
        definitions = await get_table_definitions(source_conn, table_names)

        # Create tables in replica
        for table_name in table_names:
            print(f"\nCreating table: {table_name}")

            # Drop if exists
            await replica_conn.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
            print(f"  ✓ Dropped existing table (if any)")

            # Create table
            create_stmt = definitions[table_name]["create"]
            await replica_conn.execute(create_stmt)
            print(f"  ✓ Created table structure")

            # Add primary key
            if definitions[table_name]["primary_key"]:
                pk_stmt = f"ALTER TABLE {table_name} ADD PRIMARY KEY ({definitions[table_name]['primary_key']})"
                await replica_conn.execute(pk_stmt)
                print(
                    f"  ✓ Added primary key: {definitions[table_name]['primary_key']}"
                )

            # Add unique constraint
            if definitions[table_name]["unique"]:
                unique_stmt = f"ALTER TABLE {table_name} ADD UNIQUE ({definitions[table_name]['unique']})"
                await replica_conn.execute(unique_stmt)
                print(
                    f"  ✓ Added unique constraint: {definitions[table_name]['unique']}"
                )

        print()
        print("=" * 70)
        print(" REPLICA SCHEMA SETUP COMPLETE")
        print("=" * 70)
        print()
        print("✅ All tables created successfully in replica database")
        print()

        # Verify tables exist
        replica_tables = await replica_conn.fetch(
            """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        )

        print(f"Replica now has {len(replica_tables)} tables:")
        for row in replica_tables:
            count = await replica_conn.fetchval(
                f"SELECT COUNT(*) FROM {row['table_name']}"
            )
            print(f"  - {row['table_name']}: {count} rows")

    finally:
        await source_conn.close()
        await replica_conn.close()


if __name__ == "__main__":
    asyncio.run(setup_replica())
