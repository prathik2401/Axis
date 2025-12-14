import asyncio
import json
from typing import Optional, Dict, Any, List
from datetime import datetime
from dataclasses import dataclass, asdict
from utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ReplicationCheckpoint:
    """Represents a replication checkpoint."""
    sequence_number: int
    timestamp: str
    source_db: str
    target_db: str
    tables_processed: int = 0
    last_event_id: Optional[str] = None
    status: str = "active"  # active, paused, error
    error_message: Optional[str] = None


@dataclass
class EventTracker:
    """Tracks individual event processing."""
    event_id: str
    table: str
    operation: str
    timestamp: str
    status: str  # pending, processed, failed, retrying
    retry_count: int = 0
    last_error: Optional[str] = None


class StateStore:
    """
    In-memory state store with optional persistence.
    Tracks replication state, checkpoints, and event processing status.
    
    In production, this should be backed by Redis or a persistent database.
    """
    
    def __init__(self, persistence_path: Optional[str] = None):
        """
        Initialize state store.
        
        Args:
            persistence_path: Optional file path for state persistence
        """
        self.persistence_path = persistence_path
        self._checkpoints: Dict[str, ReplicationCheckpoint] = {}
        self._events: Dict[str, EventTracker] = {}
        self._metrics: Dict[str, Any] = {
            'total_events_processed': 0,
            'total_events_failed': 0,
            'total_events_retried': 0,
            'last_checkpoint_time': None,
            'replication_lag_seconds': 0,
        }
        self._lock = asyncio.Lock()
    
    async def save_checkpoint(
        self, 
        replication_id: str, 
        checkpoint: ReplicationCheckpoint
    ) -> None:
        """
        Save a replication checkpoint.
        
        Args:
            replication_id: Unique identifier for this replication stream
            checkpoint: Checkpoint data
        """
        async with self._lock:
            self._checkpoints[replication_id] = checkpoint
            self._metrics['last_checkpoint_time'] = datetime.now().isoformat()
            
            logger.info(
                f"Checkpoint saved: {replication_id} "
                f"(seq: {checkpoint.sequence_number}, tables: {checkpoint.tables_processed})"
            )
            
            if self.persistence_path:
                await self._persist_state()
    
    async def get_checkpoint(
        self, 
        replication_id: str
    ) -> Optional[ReplicationCheckpoint]:
        """
        Retrieve a checkpoint.
        
        Args:
            replication_id: Replication stream identifier
            
        Returns:
            Checkpoint if exists, None otherwise
        """
        async with self._lock:
            return self._checkpoints.get(replication_id)
    
    async def track_event(self, event: EventTracker) -> None:
        """
        Track an event's processing status.
        
        Args:
            event: Event tracker object
        """
        async with self._lock:
            self._events[event.event_id] = event
            
            if event.status == 'processed':
                self._metrics['total_events_processed'] += 1
            elif event.status == 'failed':
                self._metrics['total_events_failed'] += 1
            elif event.status == 'retrying':
                self._metrics['total_events_retried'] += 1
    
    async def get_event_status(self, event_id: str) -> Optional[EventTracker]:
        """
        Get event processing status.
        
        Args:
            event_id: Event identifier
            
        Returns:
            Event tracker if exists
        """
        async with self._lock:
            return self._events.get(event_id)
    
    async def get_pending_events(self, limit: int = 100) -> List[EventTracker]:
        """
        Get events that are pending or need retry.
        
        Args:
            limit: Maximum number of events to return
            
        Returns:
            List of pending/failed events
        """
        async with self._lock:
            pending = [
                e for e in self._events.values()
                if e.status in ('pending', 'failed', 'retrying')
            ]
            return sorted(pending, key=lambda x: x.timestamp)[:limit]
    
    async def mark_event_processed(self, event_id: str) -> None:
        """Mark an event as successfully processed."""
        async with self._lock:
            if event_id in self._events:
                self._events[event_id].status = 'processed'
                self._metrics['total_events_processed'] += 1
    
    async def mark_event_failed(
        self, 
        event_id: str, 
        error: str,
        retry: bool = True
    ) -> None:
        """
        Mark an event as failed.
        
        Args:
            event_id: Event identifier
            error: Error message
            retry: Whether to retry this event
        """
        async with self._lock:
            if event_id in self._events:
                event = self._events[event_id]
                event.last_error = error
                event.retry_count += 1
                
                if retry and event.retry_count < 5:  # Max 5 retries
                    event.status = 'retrying'
                    self._metrics['total_events_retried'] += 1
                else:
                    event.status = 'failed'
                    self._metrics['total_events_failed'] += 1
    
    async def get_metrics(self) -> Dict[str, Any]:
        """
        Get current replication metrics.
        
        Returns:
            Dictionary of metrics
        """
        async with self._lock:
            return {
                **self._metrics,
                'total_checkpoints': len(self._checkpoints),
                'pending_events': len([
                    e for e in self._events.values() 
                    if e.status == 'pending'
                ]),
                'failed_events': len([
                    e for e in self._events.values() 
                    if e.status == 'failed'
                ]),
            }
    
    async def update_replication_lag(self, lag_seconds: float) -> None:
        """
        Update replication lag metric.
        
        Args:
            lag_seconds: Lag in seconds
        """
        async with self._lock:
            self._metrics['replication_lag_seconds'] = lag_seconds
    
    async def cleanup_old_events(self, max_age_hours: int = 24) -> int:
        """
        Clean up old processed events.
        
        Args:
            max_age_hours: Maximum age in hours for processed events
            
        Returns:
            Number of events cleaned up
        """
        async with self._lock:
            cutoff = datetime.now().timestamp() - (max_age_hours * 3600)
            
            events_to_remove = [
                event_id for event_id, event in self._events.items()
                if event.status == 'processed' 
                and datetime.fromisoformat(event.timestamp).timestamp() < cutoff
            ]
            
            for event_id in events_to_remove:
                del self._events[event_id]
            
            logger.info(f"Cleaned up {len(events_to_remove)} old events")
            return len(events_to_remove)
    
    async def _persist_state(self) -> None:
        """Persist state to disk (simple JSON file)."""
        if not self.persistence_path:
            return
        
        try:
            state = {
                'checkpoints': {
                    k: asdict(v) for k, v in self._checkpoints.items()
                },
                'metrics': self._metrics,
                'timestamp': datetime.now().isoformat(),
            }
            
            with open(self.persistence_path, 'w') as f:
                json.dump(state, f, indent=2)
                
        except Exception as e:
            logger.error(f"Failed to persist state: {e}")
    
    async def load_state(self) -> None:
        """Load state from disk."""
        if not self.persistence_path:
            return
        
        try:
            with open(self.persistence_path, 'r') as f:
                state = json.load(f)
            
            # Restore checkpoints
            self._checkpoints = {
                k: ReplicationCheckpoint(**v) 
                for k, v in state.get('checkpoints', {}).items()
            }
            
            # Restore metrics
            self._metrics.update(state.get('metrics', {}))
            
            logger.info(f"Loaded state from {self.persistence_path}")
            
        except FileNotFoundError:
            logger.info("No existing state file found")
        except Exception as e:
            logger.error(f"Failed to load state: {e}")
