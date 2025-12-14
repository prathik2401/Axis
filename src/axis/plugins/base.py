from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, AsyncIterator
from dataclasses import dataclass
import asyncio

@dataclass
class DatabaseConfig:
    """Configuration for database connection."""
    dsn: str
    pool_min_size: int = 2
    pool_max_size: int = 10
    timeout: int = 30
    ssl_required: bool = False

@dataclass
class ChangeEvent:
    """Represents a single database change event"""
    table: str
    operation: str  # 'INSERT', 'UPDATE', 'DELETE'
    payload: Dict[str, Any]
    timestamp: str
    schema: Optional[str] = None
    primary_key: Optional[Dict[str, Any]] = None
    old_values: Optional[Dict[str, Any]] = None  # For UPDATE operations

@dataclass
class ReplicationMetadata:
    """Metadata about replication state."""
    sequence_number: int # LSN or equivalent
    timestamp: str
    source_db: str
    target_db: Optional[str] = None

@dataclass
class DatabasePlugin(ABC):
    """
    Abstract base class for database plugins
    Each supported database (PostgreSQL, MySQL, etc.) implements this interface
    """
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._pool = None
        self._connected = False

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the database."""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection to the database."""
        pass

    @abstractmethod
    async def setup_replication(self, tables: List[str], channel: str) -> None:
        """
        Setup replication for specified tables.
        This includes creating triggers, functions, or configuring logical replication.
        
        Args:
            tables: List of table names to replicate
            channel: Channel/topic name for notifications
        """
        pass
    
    @abstractmethod
    def listen_changes(self, channel: str) -> AsyncIterator[ChangeEvent]:
        """
        Listen for change events from the database.
        
        Args:
            channel: Channel/topic to listen on
            
        Yields:
            ChangeEvent objects representing database changes
        """
        pass
    
    @abstractmethod
    async def apply_change(self, event: ChangeEvent) -> bool:
        """
        Apply a change event to this database (for replica databases).
        
        Args:
            event: The change event to apply
            
        Returns:
            True if successful, False otherwise
        """
        pass
    
    @abstractmethod
    async def get_schema(self, table: str) -> Dict[str, Any]:
        """
        Get schema information for a table.
        
        Args:
            table: Table name
            
        Returns:
            Schema definition including columns, types, constraints
        """
        pass
    
    @abstractmethod
    async def get_replication_position(self) -> ReplicationMetadata:
        """
        Get current replication position (LSN, timestamp, etc.).
        
        Returns:
            Current replication metadata
        """
        pass
    
    @abstractmethod
    async def set_replication_position(self, metadata: ReplicationMetadata) -> None:
        """
        Set/checkpoint replication position.
        
        Args:
            metadata: Replication metadata to save
        """
        pass
    
    @abstractmethod
    async def health_check(self) -> bool:
        """
        Check if database connection is healthy.
        
        Returns:
            True if healthy, False otherwise
        """
        pass
    
    @abstractmethod
    async def execute_batch(self, events: List[ChangeEvent]) -> int:
        """
        Apply multiple change events in a batch transaction.
        
        Args:
            events: List of change events
            
        Returns:
            Number of successfully applied events
        """
        pass
    
    @property
    def is_connected(self) -> bool:
        """Check if plugin is connected to database."""
        return self._connected
    
    @property
    @abstractmethod
    def database_type(self) -> str:
        """Return the database type (e.g., 'postgresql', 'mysql')."""
        pass
