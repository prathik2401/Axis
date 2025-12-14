"""
HTTP health check server for Axis.

Provides HTTP endpoints for:
- Health checks (/health)
- Readiness checks (/ready)
- Metrics export (/metrics)
- Status information (/status)
"""
import asyncio
from typing import Optional, Callable, Awaitable, Dict, Any
from datetime import datetime
from aiohttp import web
from axis.monitoring.metrics import get_metrics_collector
from utils.logging import get_logger

logger = get_logger(__name__)


class HealthCheckServer:
    """
    HTTP server for health checks and metrics.
    
    Provides endpoints for monitoring and observability.
    """
    
    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8080,
        health_check_callback: Optional[Callable[[], Awaitable[Dict[str, Any]]]] = None,
    ):
        """
        Initialize health check server.
        
        Args:
            host: Host to bind to
            port: Port to listen on
            health_check_callback: Optional async function that returns health status
        """
        self.host = host
        self.port = port
        self.health_check_callback = health_check_callback
        self.metrics_collector = get_metrics_collector()
        
        self.app = web.Application()
        self._setup_routes()
        self.runner: Optional[web.AppRunner] = None
        self.site: Optional[web.TCPSite] = None
        
        self.start_time = datetime.now()
    
    def _setup_routes(self) -> None:
        """Setup HTTP routes."""
        self.app.router.add_get('/health', self.health_handler)
        self.app.router.add_get('/ready', self.readiness_handler)
        self.app.router.add_get('/metrics', self.metrics_handler)
        self.app.router.add_get('/status', self.status_handler)
        self.app.router.add_get('/', self.index_handler)
    
    async def health_handler(self, request: web.Request) -> web.Response:
        """
        Health check endpoint.
        
        Returns 200 if the service is alive (basic liveness check).
        """
        return web.json_response({
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "uptime_seconds": (datetime.now() - self.start_time).total_seconds()
        })
    
    async def readiness_handler(self, request: web.Request) -> web.Response:
        """
        Readiness check endpoint.
        
        Returns 200 if the service is ready to handle requests.
        Calls the health_check_callback to determine readiness.
        """
        if self.health_check_callback:
            try:
                health_status = await self.health_check_callback()
                
                # Check if any critical components are unhealthy
                is_ready = health_status.get('status') != 'unhealthy'
                status_code = 200 if is_ready else 503
                
                return web.json_response(health_status, status=status_code)
            except Exception as e:
                logger.error(f"Health check callback failed: {e}")
                return web.json_response({
                    "status": "unhealthy",
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                }, status=503)
        
        # Default ready response if no callback provided
        return web.json_response({
            "status": "ready",
            "timestamp": datetime.now().isoformat()
        })
    
    async def metrics_handler(self, request: web.Request) -> web.Response:
        """
        Prometheus metrics endpoint.
        
        Returns metrics in Prometheus text format.
        """
        try:
            metrics_data = self.metrics_collector.export_metrics()
            return web.Response(
                body=metrics_data,
                content_type=self.metrics_collector.get_content_type()
            )
        except Exception as e:
            logger.error(f"Failed to export metrics: {e}")
            return web.Response(text=f"Error exporting metrics: {e}", status=500)
    
    async def status_handler(self, request: web.Request) -> web.Response:
        """
        Status endpoint.
        
        Returns detailed status information.
        """
        uptime = (datetime.now() - self.start_time).total_seconds()
        
        status = {
            "service": "Axis Replication System",
            "version": "0.2.0",
            "status": "running",
            "uptime_seconds": uptime,
            "start_time": self.start_time.isoformat(),
            "current_time": datetime.now().isoformat(),
            "endpoints": {
                "health": "/health",
                "readiness": "/ready",
                "metrics": "/metrics",
                "status": "/status"
            }
        }
        
        # Add health check data if available
        if self.health_check_callback:
            try:
                health_data = await self.health_check_callback()
                status["health"] = health_data
            except Exception as e:
                status["health_check_error"] = str(e)
        
        return web.json_response(status)
    
    async def index_handler(self, request: web.Request) -> web.Response:
        """
        Index endpoint.
        
        Returns information about available endpoints.
        """
        html = """
        <html>
        <head><title>Axis Health Check</title></head>
        <body>
            <h1>Axis Replication System - Health Check Server</h1>
            <h2>Available Endpoints:</h2>
            <ul>
                <li><a href="/health">/health</a> - Liveness check</li>
                <li><a href="/ready">/ready</a> - Readiness check</li>
                <li><a href="/metrics">/metrics</a> - Prometheus metrics</li>
                <li><a href="/status">/status</a> - Detailed status</li>
            </ul>
        </body>
        </html>
        """
        return web.Response(text=html, content_type='text/html')
    
    async def start(self) -> None:
        """Start the HTTP server."""
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        
        self.site = web.TCPSite(self.runner, self.host, self.port)
        await self.site.start()
        
        logger.info(f"Health check server started on http://{self.host}:{self.port}")
        logger.info(f"  - Health: http://{self.host}:{self.port}/health")
        logger.info(f"  - Ready: http://{self.host}:{self.port}/ready")
        logger.info(f"  - Metrics: http://{self.host}:{self.port}/metrics")
        logger.info(f"  - Status: http://{self.host}:{self.port}/status")
    
    async def stop(self) -> None:
        """Stop the HTTP server."""
        if self.site:
            await self.site.stop()
        
        if self.runner:
            await self.runner.cleanup()
        
        logger.info("Health check server stopped")
    
    async def run_forever(self) -> None:
        """Run the server forever."""
        await self.start()
        
        try:
            # Keep running
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()
