import os
from typing import List, Optional
from utils.logging import get_logger
from pydantic_settings import BaseSettings

logger = get_logger(__name__)


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # PostgreSQL connection settings
    pg_dsn: str = os.getenv("PG_DSN", "")
    if not pg_dsn:
        raise ValueError("PG_DSN environment variable is required")

    logger.debug(f"PostgreSQL DSN is configured")
    pg_channel: str = os.getenv("PG_CHANNEL", "axis_channel")
    logger.debug(f"PostgreSQL channel is set to: {pg_channel}")

    # RabbitMQ connection settings
    rmq_url: str = os.getenv("RMQ_URL", "")
    if not rmq_url:
        raise ValueError("RMQ_URL environment variable is required")
    logger.debug(f"RabbitMQ URL is configured")
    rmq_exchange: str = os.getenv("RMQ_EXCHANGE", "axis_exchange")
    logger.debug(f"RabbitMQ exchange is set to: {rmq_exchange}")
    rmq_exchange_type: str = "fanout"

    # Replication settings
    tables_to_replicate: List[str] = (
        os.getenv("TABLES_TO_REPLICATE", "*").split(",")
    )
    logger.debug(f"Tables to replicate: {tables_to_replicate}")
    
    # Batch settings
    batch_size: int = int(os.getenv("BATCH_SIZE", "10"))
    batch_timeout_seconds: float = float(os.getenv("BATCH_TIMEOUT", "5.0"))
    
    # Queue settings
    max_queue_size: int = int(os.getenv("MAX_QUEUE_SIZE", "10000"))
    prefetch_count: int = int(os.getenv("PREFETCH_COUNT", "10"))
    
    # State persistence
    state_file: str = os.getenv("STATE_FILE", "./axis_state.json")
    
    # App metadata
    app_name: str = "Axis"
    app_version: str = "0.1.0"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

