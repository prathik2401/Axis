"""
Example: Using Axis with Message Dispatcher

This example shows how to route different tables to different queues.
"""
import asyncio
from axis.db.dispatcher import (
    MessageDispatcher,
    RoutingRule,
    RouteAction,
    create_table_routing_rules,
    create_priority_routing_rules,
)
from axis.plugins.base import ChangeEvent


async def main():
    # Create dispatcher
    dispatcher = MessageDispatcher(
        default_queue="default_queue",
        dead_letter_queue="dlq"
    )
    
    # Register queues
    high_priority_queue = asyncio.Queue(maxsize=1000)
    normal_queue = asyncio.Queue(maxsize=5000)
    audit_queue = asyncio.Queue(maxsize=10000)
    
    dispatcher.register_queue("high_priority", high_priority_queue)
    dispatcher.register_queue("normal", normal_queue)
    dispatcher.register_queue("audit", audit_queue)
    
    # Add routing rules
    
    # Route critical tables to high priority
    priority_rules = create_priority_routing_rules(
        high_priority_tables=["users", "payments", "transactions"],
        high_priority_queue="high_priority",
        low_priority_queue="normal"
    )
    
    for rule in priority_rules:
        dispatcher.add_rule(rule)
    
    # Route all DELETE operations to audit queue
    dispatcher.add_rule(
        RoutingRule(
            table_pattern="*",
            operation="DELETE",
            action=RouteAction.ROUTE_TO,
            target_queue="audit",
            priority=15
        )
    )
    
    # Reject changes to read-only tables
    dispatcher.add_rule(
        RoutingRule(
            table_pattern="system_config",
            action=RouteAction.REJECT,
            priority=30
        )
    )
    
    # Custom condition - only route orders over $1000
    def high_value_order(event: ChangeEvent) -> bool:
        if event.table == "orders" and "amount" in event.payload:
            return float(event.payload.get("amount", 0)) > 1000
        return False
    
    dispatcher.add_rule(
        RoutingRule(
            table_pattern="orders",
            action=RouteAction.ROUTE_TO,
            target_queue="high_priority",
            priority=25,
            condition=high_value_order
        )
    )
    
    # Example: dispatch some events
    test_events = [
        ChangeEvent(
            table="users",
            operation="INSERT",
            payload={"id": 1, "email": "test@example.com"},
            timestamp="2025-12-14T00:00:00"
        ),
        ChangeEvent(
            table="orders",
            operation="INSERT",
            payload={"id": 1, "amount": "1500.00"},
            timestamp="2025-12-14T00:00:00"
        ),
        ChangeEvent(
            table="logs",
            operation="DELETE",
            payload={},
            old_values={"id": 1},
            timestamp="2025-12-14T00:00:00"
        ),
    ]
    
    for event in test_events:
        routed_to = await dispatcher.dispatch(event)
        print(f"Event {event.table}.{event.operation} routed to: {routed_to}")
    
    # Show metrics
    print("\nDispatcher Metrics:")
    print(dispatcher.get_metrics())


if __name__ == "__main__":
    asyncio.run(main())
