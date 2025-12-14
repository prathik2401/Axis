import asyncpg
import json
from typing import Any, Dict, List, Optional, AsyncIterator
from datetime import datetime
from .base import DatabasePlugin, DatabaseConfig, ChangeEvent, ReplicationMetadata
from utils.logging import get_logger

logger = get_logger(__name__)


class PostgresPlugin(DatabasePlugin):
    """PostgreSQL-specific implementation of DatabasePlugin."""

    def __init__(self, config: DatabaseConfig):
        super().__init__(config)
        self._listener_connection: Optional[asyncpg.Connection] = None

    @property
    def database_type(self) -> str:
        return "postgresql"

    async def connect(self) -> None:
        """Create connection pool to PostgreSQL database."""
        try:
            self._pool = await asyncpg.create_pool(
                self.config.dsn,
                min_size=self.config.pool_min_size,
                max_size=self.config.pool_max_size,
                command_timeout=self.config.timeout,
            )
            self._connected = True
            logger.info("Connected to PostgreSQL database.")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL database: {e}")
            raise

    async def disconnect(self) -> None:
        """Close PostgreSQL connection pool."""
        if self._listener_connection:
            try:
                await self._listener_connection.close()
            except Exception as e:
                logger.warning(f"Error closing listener connection: {e}")

        if self._pool:
            try:
                await self._pool.close()
            except Exception as e:
                logger.warning(f"Error closing connection pool: {e}")

        self._connected = False
        logger.info("Disconnected from PostgreSQL database.")

    async def setup_replication(self, tables: List[str], channel: str) -> None:
        """Setup PostgreSQL triggers and notifications function for replication.

        Args:
            tables: List of table names to monitor
            channel: PostgreSQL NOTIFY channel name
        """
        async with self._pool.acquire() as conn:
            # Create the events table if it doesn't exist
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS axis_events (
                    id SERIAL PRIMARY KEY,
                    occurred_at TIMESTAMPTZ DEFAULT NOW(),
                    table_name TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    payload JSONB
                )
            """
            )

            # Create the notification function
            await conn.execute(
                f"""
                CREATE OR REPLACE FUNCTION notify_row_change() 
                RETURNS TRIGGER AS $$
                DECLARE
                    data JSONB;
                    old_data JSONB;
                BEGIN
                    IF (TG_OP = 'DELETE') THEN
                        data := row_to_json(OLD)::JSONB;
                        old_data := NULL;
                    ELSIF (TG_OP = 'UPDATE') THEN
                        data := row_to_json(NEW)::JSONB;
                        old_data := row_to_json(OLD)::JSONB;
                    ELSE
                        data := row_to_json(NEW)::JSONB;
                        old_data := NULL;
                    END IF;

                    PERFORM pg_notify(
                        '{channel}',
                        json_build_object(
                            'table', TG_TABLE_NAME,
                            'schema', TG_TABLE_SCHEMA,
                            'operation', TG_OP,
                            'payload', data,
                            'old_values', old_data,
                            'timestamp', EXTRACT(EPOCH FROM now())
                        )::TEXT
                    );

                    IF (TG_OP = 'DELETE') THEN
                        RETURN OLD;
                    ELSE
                        RETURN NEW;
                    END IF;
                END;
                $$ LANGUAGE plpgsql;
            """
            )

            # Attach triggers to each table
            for table in tables:
                trigger_name = f"{table}_axis_trigger"

                await conn.execute(
                    f"""
                    DROP TRIGGER IF EXISTS {trigger_name} ON {table}
                """
                )

                # Create new trigger
                await conn.execute(
                    f"""
                    CREATE TRIGGER {trigger_name}
                    AFTER INSERT OR UPDATE OR DELETE ON {table}
                    FOR EACH ROW EXECUTE FUNCTION notify_row_change()
                """
                )

                logger.info(f"Setup replication trigger on table: {table}")

    async def listen_changes(self, channel: str) -> AsyncIterator[ChangeEvent]:
        """
        Listen for PostgreSQL NOTIFY events.

        Args:
            channel: PostgreSQL channel to listen on

        Yields:
            ChangeEvent objects
        """
        # Create a dedicated connection for listening
        self._listener_connection = await asyncpg.connect(self.config.dsn)

        import asyncio

        queue: asyncio.Queue = asyncio.Queue()

        def notification_callback(conn, pid, ch, payload):
            """Callback for pg_notify events."""
            try:
                data = json.loads(payload)
                event = ChangeEvent(
                    table=data["table"],
                    schema=data.get("schema"),
                    operation=data["operation"],
                    payload=data["payload"],
                    old_values=data.get("old_values"),
                    timestamp=data.get("timestamp", datetime.now().isoformat()),
                )
                asyncio.create_task(queue.put(event))
            except Exception as e:
                logger.error(f"Error processing notification: {e}")

        await self._listener_connection.add_listener(channel, notification_callback)
        logger.info(f"Listening on channel: {channel}")

        try:
            while self._connected:
                event = await queue.get()
                yield event
        finally:
            await self._listener_connection.remove_listener(
                channel, notification_callback
            )

    async def apply_change(self, event: ChangeEvent) -> bool:
        """
        Apply a change event to PostgreSQL replica.

        Args:
            event: Change event to apply

        Returns:
            True if successful
        """
        async with self._pool.acquire() as conn:
            try:
                if event.operation == "INSERT":
                    # Build INSERT statement
                    columns = list(event.payload.keys())
                    values = [event.payload[col] for col in columns]
                    placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))

                    query = f"""
                        INSERT INTO {event.table} ({', '.join(columns)})
                        VALUES ({placeholders})
                        ON CONFLICT DO NOTHING
                    """
                    await conn.execute(query, *values)

                elif event.operation == "UPDATE":
                    # Build UPDATE statement
                    set_clause = ", ".join(
                        f"{col} = ${i+1}" for i, col in enumerate(event.payload.keys())
                    )

                    # Use old_values to identify the row
                    if event.primary_key:
                        where_clause = " AND ".join(
                            f"{k} = ${len(event.payload) + i + 1}"
                            for i, k in enumerate(event.primary_key.keys())
                        )
                        values = list(event.payload.values()) + list(
                            event.primary_key.values()
                        )
                    elif event.old_values:
                        # Fallback: use all old values
                        where_clause = " AND ".join(
                            f"{k} = ${len(event.payload) + i + 1}"
                            for i, k in enumerate(event.old_values.keys())
                        )
                        values = list(event.payload.values()) + list(
                            event.old_values.values()
                        )
                    else:
                        # No way to identify the row
                        logger.error(f"Cannot update without primary_key or old_values")
                        return False

                    query = f"""
                        UPDATE {event.table}
                        SET {set_clause}
                        WHERE {where_clause}
                    """
                    await conn.execute(query, *values)

                elif event.operation == "DELETE":
                    if not event.old_values:
                        logger.error(f"Cannot delete without old_values")
                        return False
                        
                    # Build DELETE statement
                    where_clause = " AND ".join(
                        f"{k} = ${i+1}" for i, k in enumerate(event.old_values.keys())
                    )
                    values = list(event.old_values.values())

                    query = f"""
                        DELETE FROM {event.table}
                        WHERE {where_clause}
                    """
                    await conn.execute(query, *values)

                logger.debug(f"Applied {event.operation} to {event.table}")
                return True

            except Exception as e:
                logger.error(f"Failed to apply change: {e}")
                return False

    async def get_schema(self, table: str) -> Dict[str, Any]:
        """Get PostgreSQL table schema."""
        async with self._pool.acquire() as conn:
            # Get column information
            columns = await conn.fetch(
                """
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_name = $1
                ORDER BY ordinal_position
            """,
                table,
            )

            # Get primary key
            pk = await conn.fetch(
                """
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid
                    AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = $1::regclass
                    AND i.indisprimary
            """,
                table,
            )

            return {
                "table": table,
                "columns": [dict(row) for row in columns],
                "primary_key": [row["attname"] for row in pk],
            }

    async def get_replication_position(self) -> ReplicationMetadata:
        """Get current PostgreSQL LSN."""
        async with self._pool.acquire() as conn:
            lsn = await conn.fetchval("SELECT pg_current_wal_lsn()")

            return ReplicationMetadata(
                sequence_number=int(lsn.split("/")[1], 16) if lsn else 0,
                timestamp=datetime.now().isoformat(),
                source_db=self.config.dsn.split("@")[-1].split("/")[0],
            )

    async def set_replication_position(self, metadata: ReplicationMetadata) -> None:
        """Store replication checkpoint."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS axis_replication_state (
                    id INTEGER PRIMARY KEY DEFAULT 1,
                    sequence_number BIGINT,
                    timestamp TIMESTAMPTZ,
                    source_db TEXT,
                    target_db TEXT,
                    updated_at TIMESTAMPTZ DEFAULT now(),
                    CONSTRAINT single_row CHECK (id = 1)
                )
            """
            )

            await conn.execute(
                """
                INSERT INTO axis_replication_state 
                (id, sequence_number, timestamp, source_db, target_db)
                VALUES (1, $1, $2, $3, $4)
                ON CONFLICT (id) DO UPDATE
                SET sequence_number = $1,
                    timestamp = $2,
                    source_db = $3,
                    target_db = $4,
                    updated_at = now()
            """,
                metadata.sequence_number,
                metadata.timestamp,
                metadata.source_db,
                metadata.target_db,
            )

    async def health_check(self) -> bool:
        """Check PostgreSQL connection health."""
        if not self._pool:
            return False

        try:
            async with self._pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    async def execute_batch(self, events: List[ChangeEvent]) -> int:
        """Execute multiple changes in a transaction."""
        if not events:
            return 0

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                success_count = 0
                for event in events:
                    if await self.apply_change(event):
                        success_count += 1

                return success_count
