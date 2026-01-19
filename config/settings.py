import os
from typing import List, Optional, Union
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # PostgreSQL connection settings
    pg_dsn: str = Field(..., description="PostgreSQL connection DSN")
    pg_channel: str = Field(
        default="axis_channel", description="PostgreSQL NOTIFY channel"
    )

    # RabbitMQ connection settings
    rmq_url: str = Field(..., description="RabbitMQ connection URL")
    rmq_exchange: str = Field(
        default="axis_exchange", description="RabbitMQ exchange name"
    )
    rmq_exchange_type: str = Field(
        default="fanout", description="RabbitMQ exchange type"
    )

    # Replication settings - use Union to accept both string and list
    tables_to_replicate: Union[str, List[str]] = Field(
        default="*", description="Tables to replicate"
    )
    # Batch settings
    batch_size: int = Field(default=10, description="Batch size for operations")
    batch_timeout_seconds: float = Field(
        default=5.0, alias="batch_timeout", description="Batch timeout in seconds"
    )

    # Queue settings
    max_queue_size: int = Field(default=10000, description="Maximum queue size")
    prefetch_count: int = Field(default=10, description="RabbitMQ prefetch count")

    # State persistence
    state_file: str = Field(
        default="./axis_state.json", description="State persistence file"
    )

    # App metadata
    app_name: str = Field(default="Axis", description="Application name")
    app_version: str = Field(default="0.1.0", description="Application version")

    @field_validator("tables_to_replicate", mode="before")
    @classmethod
    def parse_tables(cls, v):
        """Parse comma-separated table names."""
        if isinstance(v, str):
            return [t.strip() for t in v.split(",") if t.strip()]
        return v
