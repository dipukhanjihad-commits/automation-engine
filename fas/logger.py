"""
logger.py — Structured JSON logging for FAS.
"""

import json
import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Optional


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "event": getattr(record, "event", "log"),
            "message": record.getMessage(),
        }
        for field in ("folder", "file", "status", "attempt", "latency_ms", "error", "detail"):
            val = getattr(record, field, None)
            if val is not None:
                payload[field] = val
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def setup_logger(level: str = "INFO", log_file: Optional[str] = None) -> logging.Logger:
    logger = logging.getLogger("fas")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.handlers.clear()
    fmt = JsonFormatter()
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        fh = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    logger.propagate = False
    return logger


def get_logger() -> logging.Logger:
    return logging.getLogger("fas")


def log_event(
    event: str,
    status: str = "ok",
    folder: Optional[str] = None,
    file: Optional[str] = None,
    attempt: Optional[int] = None,
    latency_ms: Optional[float] = None,
    error: Optional[str] = None,
    **extra: Any,
) -> None:
    logger = get_logger()
    parts = [event, status]
    if folder:
        parts.append(f"[{folder}]")
    if file:
        parts.append(file)
    msg = " | ".join(parts)

    kwargs: dict[str, Any] = {
        "extra": {
            "event": event,
            "status": status,
            "folder": folder,
            "file": file,
            "attempt": attempt,
            "latency_ms": round(latency_ms, 2) if latency_ms is not None else None,
            "error": error,
            **extra,
        }
    }
    if status in ("error", "failed"):
        logger.error(msg, **kwargs)
    elif status == "warn":
        logger.warning(msg, **kwargs)
    else:
        logger.info(msg, **kwargs)
