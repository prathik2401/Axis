"""
Axis CLI - Command-line interface for the replication system.

Usage:
    python cli.py                    # Run with full orchestrator
    python cli.py --simple           # Run in simple publisher mode
    python cli.py --help             # Show help
"""
import asyncio
import argparse
import sys
from dotenv import load_dotenv
from config.settings import Settings
from axis.replication.service import run, run_simple
from utils.logging import get_logger

logger = get_logger(__name__)


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Axis - Async PostgreSQL Replication System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument(
        "--simple",
        action="store_true",
        help="Run in simple publisher-only mode (no orchestrator)"
    )
    
    parser.add_argument(
        "--replicas",
        type=str,
        nargs="+",
        help="List of replica database DSNs"
    )
    
    parser.add_argument(
        "--http-port",
        type=int,
        default=8080,
        help="Port for HTTP health check server (default: 8080)"
    )
    
    parser.add_argument(
        "--no-http",
        action="store_true",
        help="Disable HTTP health check server"
    )
    
    parser.add_argument(
        "--env-file",
        type=str,
        default=".env",
        help="Path to .env file (default: .env)"
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version="Axis 0.1.0"
    )
    
    return parser.parse_args()


async def main():
    """Main entry point."""
    args = parse_args()
    
    # Load environment variables
    load_dotenv(args.env_file)
    
    try:
        settings = Settings()
        
        logger.info("=" * 60)
        logger.info("AXIS - Async PostgreSQL Replication System")
        logger.info("=" * 60)
        logger.info(f"Mode: {'Simple Publisher' if args.simple else 'Full Orchestrator'}")
        logger.info(f"Source DB: {settings.pg_dsn.split('@')[-1]}")
        logger.info(f"RabbitMQ: {settings.rmq_url}")
        logger.info(f"Channel: {settings.pg_channel}")
        if not args.simple and not args.no_http:
            logger.info(f"HTTP Server: http://localhost:{args.http_port}")
            logger.info(f"  - Health: http://localhost:{args.http_port}/health")
            logger.info(f"  - Metrics: http://localhost:{args.http_port}/metrics")
        logger.info("=" * 60)
        
        if args.simple:
            # Run simple mode
            await run_simple(settings)
        else:
            # Run full orchestrator mode
            await run(
                settings,
                replica_dsns=args.replicas,
                enable_http_server=not args.no_http,
                http_port=args.http_port
            )
            
    except KeyboardInterrupt:
        logger.info("\nShutdown requested by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

