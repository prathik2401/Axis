"""Structured logging utilities for the Axis replication platform."""

from __future__ import annotations

import json
import logging
import os
import sys
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator


_GLOBAL_CONTEXT: ContextVar[dict[str, Any]] = ContextVar(
    "axis_global_context", default={}
)


def _coerce_level(level: str) -> int:
    candidate = logging.getLevelName(level.upper())
    if isinstance(candidate, int):
        return candidate
    raise ValueError(f"Unsupported log level: {level}")


def _serialize_mapping(mapping: Dict[str, Any]) -> Dict[str, Any]:
    sanitized: Dict[str, Any] = {}
    for key, value in mapping.items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            sanitized[key] = value
        elif isinstance(value, datetime):
            sanitized[key] = value.isoformat()
        else:
            sanitized[key] = repr(value)
    return sanitized


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _supports_color(stream: Any) -> bool:
    return bool(getattr(stream, "isatty", lambda: False)())


def _color_for_level(level: int) -> str:
    if level >= logging.ERROR:
        return "31"  # red
    if level >= logging.WARNING:
        return "33"  # yellow
    if level >= logging.INFO:
        return "32"  # green
    return "36"  # cyan


@dataclass(slots=True)
class LoggerConfiguration:
    """Represents the resolved runtime logging configuration."""

    service_name: str
    level: int
    json_logs: bool
    colorize: bool
    sink: str = "stdout"
    log_file: Path | None = None


class AxisJSONFormatter(logging.Formatter):
    """Formats log records as single-line JSON documents."""

    def __init__(self, *, service_name: str) -> None:
        super().__init__()
        self._service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "timestamp": _now_iso(),
            "level": record.levelname,
            "service": self._service_name,
            "logger": record.name,
            "message": record.getMessage(),
        }
        context = getattr(record, "axis_context", None)
        if context:
            payload.update(_serialize_mapping(context))
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"))


class AxisTextFormatter(logging.Formatter):
    """Formats log records as human-friendly single lines."""

    def __init__(self, *, service_name: str, colorize: bool) -> None:
        super().__init__()
        self._service_name = service_name
        self._colorize = colorize and _supports_color(sys.stdout)

    def format(self, record: logging.LogRecord) -> str:
        timestamp = _now_iso()
        base = f"[{timestamp}] {record.levelname:<8} {self._service_name} {record.name} | {record.getMessage()}"
        context = getattr(record, "axis_context", None)
        if context:
            extras = " ".join(
                f"{key}={value}"
                for key, value in sorted(_serialize_mapping(context).items())
            )
            if extras:
                base = f"{base} :: {extras}"
        if record.exc_info:
            base = f"{base}\n{self.formatException(record.exc_info)}"
        if self._colorize:
            color = _color_for_level(record.levelno)
            return f"\033[{color}m{base}\033[0m"
        return base


class AxisLogger:
    """High-level logging facade providing structured, contextual logging."""

    _configured: bool = False
    _config: LoggerConfiguration | None = None

    def __init__(self, name: str, *, context: dict[str, Any] | None = None) -> None:
        self._logger = logging.getLogger(name)
        self._context = context or {}

    @classmethod
    def configure(
        cls,
        *,
        service_name: str | None = None,
        level: str | None = None,
        json_logs: bool | None = None,
        colorize: bool | None = None,
        sink: str = "stdout",
        log_file: str | None = None,
    ) -> None:
        if cls._configured:
            return

        resolved = LoggerConfiguration(
            service_name=service_name or os.getenv("LOG_SERVICE_NAME", "axis"),
            level=_coerce_level(level or os.getenv("LOG_LEVEL", "INFO")),
            json_logs=(
                json_logs
                if json_logs is not None
                else os.getenv("LOG_FORMAT", "json").lower() == "json"
            ),
            colorize=(
                colorize
                if colorize is not None
                else os.getenv("LOG_COLOR", "false").lower() in {"1", "true", "yes"}
            ),
            sink=sink,
            log_file=Path(log_file) if log_file else _resolve_log_file_from_env(),
        )

        handler: logging.Handler
        if resolved.log_file:
            handler = logging.FileHandler(resolved.log_file, mode="a", encoding="utf-8")
        elif resolved.sink == "stderr":
            handler = logging.StreamHandler(sys.stderr)
        else:
            handler = logging.StreamHandler(sys.stdout)

        handler.setFormatter(
            AxisJSONFormatter(service_name=resolved.service_name)
            if resolved.json_logs
            else AxisTextFormatter(
                service_name=resolved.service_name, colorize=resolved.colorize
            )
        )

        logging.basicConfig(level=resolved.level, handlers=[handler], force=True)

        cls._configured = True
        cls._config = resolved

    @classmethod
    def reconfigure(cls, **kwargs: Any) -> None:
        cls._configured = False
        cls.configure(**kwargs)

    @classmethod
    @contextmanager
    def context(cls, **values: Any) -> Iterator[None]:
        sanitized = _serialize_mapping(values)
        current = dict(_GLOBAL_CONTEXT.get())
        current.update(sanitized)
        token = _GLOBAL_CONTEXT.set(current)
        try:
            yield
        finally:
            _GLOBAL_CONTEXT.reset(token)

    def bind(self, **values: Any) -> "AxisLogger":
        merged = dict(self._context)
        merged.update(_serialize_mapping(values))
        return AxisLogger(self.name, context=merged)

    @property
    def name(self) -> str:
        return self._logger.name

    def setLevel(
        self, level: int | str
    ) -> None:  # noqa: N802 - mirror stdlib signature
        target = _coerce_level(level) if isinstance(level, str) else level
        self._logger.setLevel(target)

    def debug(self, message: str, **context: Any) -> None:
        self._log(logging.DEBUG, message, context)

    def info(self, message: str, **context: Any) -> None:
        self._log(logging.INFO, message, context)

    def warning(self, message: str, **context: Any) -> None:
        self._log(logging.WARNING, message, context)

    def error(self, message: str, **context: Any) -> None:
        self._log(logging.ERROR, message, context)

    def critical(self, message: str, **context: Any) -> None:
        self._log(logging.CRITICAL, message, context)

    def exception(self, message: str, **context: Any) -> None:
        self._log(logging.ERROR, message, context, exc_info=True)

    def _log(
        self,
        level: int,
        message: str,
        context: Dict[str, Any],
        *,
        exc_info: bool = False,
    ) -> None:
        if not self._configured:
            AxisLogger.configure()
        current = dict(_GLOBAL_CONTEXT.get())
        current.update(self._context)
        current.update(_serialize_mapping(context))
        self._logger.log(
            level, message, extra={"axis_context": current}, exc_info=exc_info
        )


def _resolve_log_file_from_env() -> Path | None:
    candidate = os.getenv("LOG_FILE")
    if candidate:
        path = Path(candidate).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        return path
    return None


def get_logger(name: str | None = None) -> AxisLogger:
    AxisLogger.configure()
    logger_name = name or os.getenv("LOG_DEFAULT_NAMESPACE", "axis")
    return AxisLogger(logger_name)


__all__ = [
    "AxisLogger",
    "AxisJSONFormatter",
    "AxisTextFormatter",
    "LoggerConfiguration",
    "get_logger",
]
