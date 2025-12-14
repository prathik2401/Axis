import asyncio
from typing import Optional, Callable, Awaitable
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
from utils.logging import get_logger

logger = get_logger(__name__)


class PressureLevel(Enum):
    """System pressure levels."""
    NORMAL = "normal"
    MODERATE = "moderate"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class BackpressureConfig:
    """Configuration for backpressure management."""
    # Queue size thresholds (percentage of max)
    moderate_threshold: float = 0.6  # 60%
    high_threshold: float = 0.8  # 80%
    critical_threshold: float = 0.95  # 95%
    
    # Rate limiting
    max_events_per_second: int = 1000
    
    # Adaptive batching
    min_batch_size: int = 10
    max_batch_size: int = 100
    
    # Monitoring
    check_interval_seconds: float = 5.0


class BackpressureManager:
    """
    Manages backpressure in the replication pipeline.
    
    Features:
    - Queue size monitoring
    - Adaptive rate limiting
    - Dynamic batch size adjustment
    - Circuit breaker pattern
    - Alert callbacks
    """
    
    def __init__(
        self,
        config: BackpressureConfig,
        max_queue_size: int = 10000,
        alert_callback: Optional[Callable[[PressureLevel, str], Awaitable[None]]] = None,
    ):
        """
        Initialize backpressure manager.
        
        Args:
            config: Backpressure configuration
            max_queue_size: Maximum queue size
            alert_callback: Optional async callback for alerts
        """
        self.config = config
        self.max_queue_size = max_queue_size
        self.alert_callback = alert_callback
        
        self._current_pressure = PressureLevel.NORMAL
        self._current_queue_size = 0
        self._events_processed_per_second = 0
        self._last_check_time = datetime.now()
        self._last_event_count = 0
        
        # Rate limiting
        self._rate_limiter_tokens = config.max_events_per_second
        self._rate_limiter_last_refill = datetime.now()
        
        # Adaptive batching
        self._current_batch_size = config.min_batch_size
        
        # Circuit breaker
        self._circuit_open = False
        self._circuit_failures = 0
        self._circuit_last_failure_time: Optional[datetime] = None
        
        self._lock = asyncio.Lock()
        self._monitoring_task: Optional[asyncio.Task] = None
    
    async def start_monitoring(self) -> None:
        """Start background monitoring task."""
        if self._monitoring_task is None:
            self._monitoring_task = asyncio.create_task(self._monitor_loop())
            logger.info("Backpressure monitoring started")
    
    async def stop_monitoring(self) -> None:
        """Stop background monitoring task."""
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
            self._monitoring_task = None
            logger.info("Backpressure monitoring stopped")
    
    async def update_queue_size(self, size: int) -> None:
        """
        Update current queue size.
        
        Args:
            size: Current queue size
        """
        async with self._lock:
            self._current_queue_size = size
            await self._evaluate_pressure()
    
    async def can_accept_event(self) -> bool:
        """
        Check if system can accept a new event (rate limiting).
        
        Returns:
            True if event can be accepted
        """
        if self._circuit_open:
            return False
        
        async with self._lock:
            # Refill rate limiter tokens
            now = datetime.now()
            time_passed = (now - self._rate_limiter_last_refill).total_seconds()
            
            if time_passed >= 1.0:
                self._rate_limiter_tokens = self.config.max_events_per_second
                self._rate_limiter_last_refill = now
            
            # Check if we have tokens
            if self._rate_limiter_tokens > 0:
                self._rate_limiter_tokens -= 1
                return True
            
            return False
    
    async def wait_if_needed(self, timeout: float = 30.0) -> bool:
        """
        Wait if system is under pressure.
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            True if can proceed, False if timeout reached
        """
        start_time = datetime.now()
        
        while True:
            if await self.can_accept_event():
                return True
            
            elapsed = (datetime.now() - start_time).total_seconds()
            if elapsed >= timeout:
                logger.warning("Backpressure wait timeout reached")
                return False
            
            # Wait with exponential backoff based on pressure
            if self._current_pressure == PressureLevel.MODERATE:
                await asyncio.sleep(0.1)
            elif self._current_pressure == PressureLevel.HIGH:
                await asyncio.sleep(0.5)
            elif self._current_pressure == PressureLevel.CRITICAL:
                await asyncio.sleep(1.0)
            else:
                await asyncio.sleep(0.01)
    
    async def get_recommended_batch_size(self) -> int:
        """
        Get recommended batch size based on current pressure.
        
        Returns:
            Recommended batch size
        """
        async with self._lock:
            return self._current_batch_size
    
    async def record_success(self) -> None:
        """Record successful event processing."""
        async with self._lock:
            if self._circuit_open:
                # Reset circuit breaker on success
                self._circuit_failures = 0
                self._circuit_open = False
                logger.info("Circuit breaker closed")
    
    async def record_failure(self) -> None:
        """Record failed event processing."""
        async with self._lock:
            self._circuit_failures += 1
            self._circuit_last_failure_time = datetime.now()
            
            # Open circuit breaker after 5 consecutive failures
            if self._circuit_failures >= 5:
                self._circuit_open = True
                logger.error("Circuit breaker opened due to failures")
                
                if self.alert_callback:
                    await self.alert_callback(
                        PressureLevel.CRITICAL,
                        "Circuit breaker opened"
                    )
    
    async def get_pressure_level(self) -> PressureLevel:
        """Get current pressure level."""
        async with self._lock:
            return self._current_pressure
    
    async def get_metrics(self) -> dict:
        """Get backpressure metrics."""
        async with self._lock:
            queue_utilization = (
                self._current_queue_size / self.max_queue_size
                if self.max_queue_size > 0
                else 0
            )
            
            return {
                'pressure_level': self._current_pressure.value,
                'queue_size': self._current_queue_size,
                'queue_utilization': queue_utilization,
                'events_per_second': self._events_processed_per_second,
                'current_batch_size': self._current_batch_size,
                'circuit_open': self._circuit_open,
                'circuit_failures': self._circuit_failures,
                'rate_limiter_tokens': self._rate_limiter_tokens,
            }
    
    async def _evaluate_pressure(self) -> None:
        """Evaluate current pressure level based on queue size."""
        utilization = (
            self._current_queue_size / self.max_queue_size
            if self.max_queue_size > 0
            else 0
        )
        
        old_pressure = self._current_pressure
        
        if utilization >= self.config.critical_threshold:
            self._current_pressure = PressureLevel.CRITICAL
            self._current_batch_size = self.config.max_batch_size
        elif utilization >= self.config.high_threshold:
            self._current_pressure = PressureLevel.HIGH
            self._current_batch_size = min(
                self.config.max_batch_size,
                self._current_batch_size + 10
            )
        elif utilization >= self.config.moderate_threshold:
            self._current_pressure = PressureLevel.MODERATE
            # Keep current batch size
        else:
            self._current_pressure = PressureLevel.NORMAL
            self._current_batch_size = self.config.min_batch_size
        
        # Alert on pressure change
        if old_pressure != self._current_pressure:
            logger.warning(
                f"Pressure level changed: {old_pressure.value} -> "
                f"{self._current_pressure.value} (utilization: {utilization:.1%})"
            )
            
            if self.alert_callback:
                await self.alert_callback(
                    self._current_pressure,
                    f"Queue utilization: {utilization:.1%}"
                )
    
    async def _monitor_loop(self) -> None:
        """Background monitoring loop."""
        while True:
            try:
                await asyncio.sleep(self.config.check_interval_seconds)
                
                async with self._lock:
                    # Calculate events per second
                    now = datetime.now()
                    time_diff = (now - self._last_check_time).total_seconds()
                    
                    if time_diff > 0:
                        # This would need to be updated by the consumer
                        # For now, just keep the last known value
                        pass
                    
                    self._last_check_time = now
                    
                    # Re-evaluate pressure
                    await self._evaluate_pressure()
                    
                    # Check circuit breaker timeout (auto-close after 60s)
                    if self._circuit_open and self._circuit_last_failure_time:
                        time_since_failure = (
                            now - self._circuit_last_failure_time
                        ).total_seconds()
                        
                        if time_since_failure > 60:
                            self._circuit_failures = 0
                            self._circuit_open = False
                            logger.info("Circuit breaker auto-closed after timeout")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in backpressure monitor: {e}")
