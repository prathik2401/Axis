"""
Example: Using Axis Replication with Transformations

This example shows how to use the transformer to mask PII data
during replication.
"""
import asyncio
from axis.replication.orchestrator import ReplicationOrchestrator
from axis.plugins.base import DatabaseConfig
from axis.replication.state_store import StateStore
from axis.replication.transformer import (
    DataTransformer,
    TransformationRule,
    TransformationType,
    create_pii_masking_rules,
)


async def main():
    # Configure databases
    source_config = DatabaseConfig(
        dsn="postgresql://user:password@localhost:5432/source_db"
    )
    
    replica_config = DatabaseConfig(
        dsn="postgresql://user:password@localhost:5433/replica_db"
    )
    
    # Create transformer with PII masking
    transformer = DataTransformer()
    
    # Add PII masking rules
    pii_rules = create_pii_masking_rules(tables=["users", "customers"])
    for rule in pii_rules:
        transformer.add_rule(rule)
    
    # Add custom transformation - convert timestamps
    transformer.add_rule(
        TransformationRule(
            transformation_type=TransformationType.CONVERT_TYPE,
            table="orders",
            column="created_at",
            parameters={"type": "str"}
        )
    )
    
    # Add custom column filtering
    transformer.add_rule(
        TransformationRule(
            transformation_type=TransformationType.FILTER_COLUMN,
            table="users",
            column="internal_notes"
        )
    )
    
    # Create state store
    state_store = StateStore(persistence_path="./axis_state.json")
    
    # Create orchestrator
    orchestrator = ReplicationOrchestrator(
        source_db_config=source_config,
        rabbitmq_url="amqp://guest:guest@localhost:5672/",
        exchange_name="axis_exchange",
        channel_name="axis_channel",
        state_store=state_store,
        tables_to_replicate=["users", "customers", "orders"],
        replica_configs=[replica_config],
    )
    
    # Run the orchestrator
    await orchestrator.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
