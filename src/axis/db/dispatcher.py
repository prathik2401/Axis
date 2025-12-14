from typing import Dict, List, Optional, Set, Callable, Awaitable, Any
from dataclasses import dataclass
from enum import Enum
import asyncio
from axis.plugins.base import ChangeEvent
from utils.logging import get_logger

logger = get_logger(__name__)


class RouteAction(Enum):
    """Actions for routing."""
    ACCEPT = "accept"
    REJECT = "reject"
    ROUTE_TO = "route_to"


@dataclass
class RoutingRule:
    """Defines a routing rule."""
    table_pattern: str  # Table name or pattern (* for all)
    operation: Optional[str] = None  # INSERT, UPDATE, DELETE, or None for all
    action: RouteAction = RouteAction.ACCEPT
    target_queue: Optional[str] = None
    priority: int = 0  # Higher priority rules evaluated first
    condition: Optional[Callable[[ChangeEvent], bool]] = None


class MessageDispatcher:
    """
    Routes change events to appropriate queues/handlers based on rules.
    
    Features:
    - Table-based routing
    - Operation filtering
    - Priority queues
    - Custom conditions
    - Dead letter handling
    - Multi-cast routing
    """
    
    def __init__(
        self,
        default_queue: str = "default",
        dead_letter_queue: str = "dlq"
    ):
        """
        Initialize dispatcher.
        
        Args:
            default_queue: Default queue for unmatched events
            dead_letter_queue: Queue for rejected/failed events
        """
        self.default_queue = default_queue
        self.dead_letter_queue = dead_letter_queue
        
        # Routing rules sorted by priority
        self._rules: List[RoutingRule] = []
        
        # Queue handlers
        self._queue_handlers: Dict[str, asyncio.Queue] = {}
        
        # Metrics
        self._routed_count: Dict[str, int] = {}
        self._rejected_count = 0
        self._default_count = 0
    
    def add_rule(self, rule: RoutingRule) -> None:
        """
        Add a routing rule.
        
        Args:
            rule: Routing rule to add
        """
        self._rules.append(rule)
        # Re-sort by priority (descending)
        self._rules.sort(key=lambda r: r.priority, reverse=True)
        logger.info(f"Added routing rule for table '{rule.table_pattern}'")
    
    def register_queue(self, queue_name: str, queue: asyncio.Queue) -> None:
        """
        Register a queue for routing.
        
        Args:
            queue_name: Name of the queue
            queue: AsyncIO queue instance
        """
        self._queue_handlers[queue_name] = queue
        self._routed_count[queue_name] = 0
        logger.info(f"Registered queue: {queue_name}")
    
    async def dispatch(self, event: ChangeEvent) -> List[str]:
        """
        Dispatch an event based on routing rules.
        
        Args:
            event: Change event to dispatch
            
        Returns:
            List of queue names the event was routed to
        """
        routed_to = []
        matched_any = False
        
        # Evaluate rules in priority order
        for rule in self._rules:
            if self._matches_rule(event, rule):
                matched_any = True
                
                if rule.action == RouteAction.ACCEPT:
                    # Route to specified queue or default
                    queue_name = rule.target_queue or self.default_queue
                    await self._route_to_queue(event, queue_name)
                    routed_to.append(queue_name)
                    break  # Stop after first match
                
                elif rule.action == RouteAction.REJECT:
                    # Send to dead letter queue
                    await self._route_to_queue(event, self.dead_letter_queue)
                    routed_to.append(self.dead_letter_queue)
                    self._rejected_count += 1
                    logger.warning(
                        f"Event rejected: {event.table}.{event.operation}"
                    )
                    break
                
                elif rule.action == RouteAction.ROUTE_TO:
                    # Route to specific queue (can match multiple)
                    if rule.target_queue:
                        await self._route_to_queue(event, rule.target_queue)
                        routed_to.append(rule.target_queue)
        
        # If no rules matched, use default
        if not matched_any:
            await self._route_to_queue(event, self.default_queue)
            routed_to.append(self.default_queue)
            self._default_count += 1
        
        return routed_to
    
    def _matches_rule(self, event: ChangeEvent, rule: RoutingRule) -> bool:
        """
        Check if event matches a routing rule.
        
        Args:
            event: Change event
            rule: Routing rule
            
        Returns:
            True if event matches rule
        """
        # Check table pattern
        if rule.table_pattern != "*":
            if not self._matches_pattern(event.table, rule.table_pattern):
                return False
        
        # Check operation
        if rule.operation and rule.operation != event.operation:
            return False
        
        # Check custom condition
        if rule.condition:
            try:
                if not rule.condition(event):
                    return False
            except Exception as e:
                logger.error(f"Error evaluating routing condition: {e}")
                return False
        
        return True
    
    def _matches_pattern(self, table: str, pattern: str) -> bool:
        """
        Check if table name matches pattern.
        
        Args:
            table: Table name
            pattern: Pattern (supports * wildcard)
            
        Returns:
            True if matches
        """
        if pattern == "*":
            return True
        
        if "*" in pattern:
            # Simple wildcard matching
            import re
            regex = pattern.replace("*", ".*")
            return bool(re.match(f"^{regex}$", table))
        
        return table == pattern
    
    async def _route_to_queue(self, event: ChangeEvent, queue_name: str) -> None:
        """
        Route event to a specific queue.
        
        Args:
            event: Change event
            queue_name: Target queue name
        """
        if queue_name not in self._queue_handlers:
            logger.warning(
                f"Queue '{queue_name}' not registered, using default"
            )
            queue_name = self.default_queue
            
            if queue_name not in self._queue_handlers:
                logger.error(f"Default queue not registered!")
                return
        
        queue = self._queue_handlers[queue_name]
        
        try:
            await queue.put(event)
            self._routed_count[queue_name] = (
                self._routed_count.get(queue_name, 0) + 1
            )
        except asyncio.QueueFull:
            logger.error(f"Queue '{queue_name}' is full, dropping event")
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get dispatcher metrics.
        
        Returns:
            Metrics dictionary
        """
        return {
            'routed_by_queue': self._routed_count.copy(),
            'rejected_count': self._rejected_count,
            'default_count': self._default_count,
            'total_rules': len(self._rules),
            'registered_queues': list(self._queue_handlers.keys()),
        }
    
    def clear_rules(self) -> None:
        """Clear all routing rules."""
        self._rules.clear()
        logger.info("Cleared all routing rules")


# Predefined routing rule builders

def create_table_routing_rules(
    table_to_queue_mapping: Dict[str, str]
) -> List[RoutingRule]:
    """
    Create simple table-to-queue routing rules.
    
    Args:
        table_to_queue_mapping: Dict mapping table names to queue names
        
    Returns:
        List of routing rules
    """
    rules = []
    
    for table, queue in table_to_queue_mapping.items():
        rules.append(
            RoutingRule(
                table_pattern=table,
                action=RouteAction.ROUTE_TO,
                target_queue=queue,
                priority=10
            )
        )
    
    return rules


def create_operation_routing_rules(
    operations_to_queue: Dict[str, str]
) -> List[RoutingRule]:
    """
    Create operation-based routing rules.
    
    Args:
        operations_to_queue: Dict mapping operations to queue names
        
    Returns:
        List of routing rules
    """
    rules = []
    
    for operation, queue in operations_to_queue.items():
        rules.append(
            RoutingRule(
                table_pattern="*",
                operation=operation,
                action=RouteAction.ROUTE_TO,
                target_queue=queue,
                priority=5
            )
        )
    
    return rules


def create_priority_routing_rules(
    high_priority_tables: List[str],
    high_priority_queue: str,
    low_priority_queue: str
) -> List[RoutingRule]:
    """
    Create priority-based routing rules.
    
    Args:
        high_priority_tables: Tables to route to high priority queue
        high_priority_queue: Queue for high priority tables
        low_priority_queue: Queue for other tables
        
    Returns:
        List of routing rules
    """
    rules = []
    
    # High priority tables
    for table in high_priority_tables:
        rules.append(
            RoutingRule(
                table_pattern=table,
                action=RouteAction.ROUTE_TO,
                target_queue=high_priority_queue,
                priority=20
            )
        )
    
    # Low priority default
    rules.append(
        RoutingRule(
            table_pattern="*",
            action=RouteAction.ROUTE_TO,
            target_queue=low_priority_queue,
            priority=1
        )
    )
    
    return rules
