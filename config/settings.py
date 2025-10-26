import os
from utils.logging import get_logger
from pydantic_settings import BaseSettings

logger = get_logger(__name__)


class Settings(BaseSettings):
    # PostgreSQL connection settings
    pg_dsn: str = os.getenv("PG_DSN", "")
    if not pg_dsn:
        raise ValueError("PG_DSN environment variable is required")

    logger.debug(f"PostgreSQL DSN is configured as: {pg_dsn}")
    pg_channel = os.getenv("PG_CHANNEL", "axis_channel")
    logger.debug(f"PostgreSQL channel is set to: {pg_channel}")

    # RabbitMQ connection settings
    rmq_url: str = os.getenv("RMQ_URL", "")
    if not rmq_url:
        raise ValueError("RMQ_URL environment variable is required")
    logger.debug(f"RabbitMQ URL is configured as: {rmq_url}")
    rmq_exchange: str = os.getenv("RMQ_EXCHANGE", "axis_exchange")
    logger.debug(f"RabbitMQ exchange is set to: {rmq_exchange}")
    rmq_exchange_type: str = "fanout"

    # app
    app_name: str = "Axis"
    batch_size: int = 100

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
