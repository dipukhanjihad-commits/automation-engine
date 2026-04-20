"""
config_loader.py — Runtime-reloadable multi-folder configuration for FAS.

Each entry in config["folders"] is an independent FolderProfile with its own
watch path, upload settings, metadata, and retry policy. All profiles share
one SQLite database; each gets its own table named after the folder "name".
"""

import json
import os
import re
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Locate config.json (PyInstaller-compatible)
# ---------------------------------------------------------------------------

def _find_config_path(override: Optional[str] = None) -> Path:
    if override:
        return Path(override)
    env_path = os.environ.get("FAS_CONFIG")
    if env_path:
        return Path(env_path)
    if getattr(sys, "frozen", False):
        exe_dir = Path(sys.executable).parent
    else:
        exe_dir = Path(__file__).parent.parent
    candidate = exe_dir / "config.json"
    if candidate.exists():
        return candidate
    return Path.cwd() / "config.json"


def _safe_table_name(name: str) -> str:
    """Convert a folder name to a valid SQLite table identifier."""
    safe = re.sub(r"[^a-zA-Z0-9_]", "_", name).strip("_").lower()
    if not safe:
        safe = "folder"
    if safe[0].isdigit():
        safe = f"f_{safe}"
    return safe


# ---------------------------------------------------------------------------
# Per-folder profile
# ---------------------------------------------------------------------------

@dataclass
class FolderProfile:
    name: str
    table_name: str
    watch_path: Path
    watch_depth: int
    watch_extensions: Optional[list[str]]
    upload_type: str
    ftp_config: dict[str, Any]
    api_endpoint: str
    metadata: dict[str, Any]
    retry_interval: int
    retry_max_attempts: int

    @classmethod
    def from_dict(cls, data: dict) -> "FolderProfile":
        name = str(data.get("name", "folder")).strip()
        watch = data.get("watch", {})
        upload = data.get("upload", {})
        retry = data.get("retry", {})
        exts_raw = watch.get("extensions")
        exts: Optional[list[str]] = None
        if exts_raw:
            exts = [e.lower() if e.startswith(".") else f".{e.lower()}" for e in exts_raw]
        return cls(
            name=name,
            table_name=_safe_table_name(name),
            watch_path=Path(watch.get("path", ".")),
            watch_depth=int(watch.get("depth", 1)),
            watch_extensions=exts,
            upload_type=upload.get("type", "ftp").lower(),
            ftp_config=dict(upload.get("ftp", {})),
            api_endpoint=upload.get("endpoint", ""),
            metadata=dict(data.get("metadata", {})),
            retry_interval=int(retry.get("interval_seconds", 60)),
            retry_max_attempts=int(retry.get("max_attempts", 5)),
        )

    def validate(self) -> list[str]:
        errors: list[str] = []
        if not self.name:
            errors.append("folder name is required")
        if self.upload_type not in ("ftp", "api"):
            errors.append(f"[{self.name}] upload.type must be 'ftp' or 'api'")
        if self.upload_type == "ftp":
            for f in ("host", "user", "password"):
                if not self.ftp_config.get(f):
                    errors.append(f"[{self.name}] upload.ftp.{f} is required")
        if self.upload_type == "api" and not self.api_endpoint:
            errors.append(f"[{self.name}] upload.endpoint is required")
        if self.retry_max_attempts < 1:
            errors.append(f"[{self.name}] retry.max_attempts must be >= 1")
        if self.retry_interval < 1:
            errors.append(f"[{self.name}] retry.interval_seconds must be >= 1")
        return errors


# ---------------------------------------------------------------------------
# Top-level Config
# ---------------------------------------------------------------------------

class Config:
    """Thread-safe config with hot-reload and multi-folder profile list."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.RLock()
        self._data: dict[str, Any] = {}
        self._mtime: float = 0.0
        self._profiles: list[FolderProfile] = []
        self.reload()

    def reload(self) -> bool:
        mtime = self._path.stat().st_mtime
        if mtime == self._mtime:
            return False
        with open(self._path, encoding="utf-8") as fh:
            data = json.load(fh)
        profiles = [FolderProfile.from_dict(f) for f in data.get("folders", [])]
        with self._lock:
            self._data = data
            self._profiles = profiles
            self._mtime = mtime
        return True

    def get(self, *keys: str, default: Any = None) -> Any:
        with self._lock:
            node = self._data
            for k in keys:
                if not isinstance(node, dict):
                    return default
                node = node.get(k, default)
                if node is default:
                    return default
            return node

    @property
    def profiles(self) -> list[FolderProfile]:
        with self._lock:
            return list(self._profiles)

    @property
    def db_path(self) -> str:
        return self.get("database", "path", default="fas.db")

    @property
    def log_level(self) -> str:
        return self.get("logging", "level", default="INFO")

    @property
    def log_file(self) -> Optional[str]:
        return self.get("logging", "file", default=None)

    def validate(self) -> list[str]:
        errors: list[str] = []
        if not self._profiles:
            errors.append("No folders defined in config")
        names = [p.name for p in self._profiles]
        if len(names) != len(set(names)):
            errors.append("Folder names must be unique")
        for profile in self._profiles:
            errors.extend(profile.validate())
        return errors


# ---------------------------------------------------------------------------
# Module-level singleton + background watcher
# ---------------------------------------------------------------------------

_config_instance: Optional[Config] = None
_config_lock = threading.Lock()


def init_config(override_path: Optional[str] = None) -> Config:
    global _config_instance
    with _config_lock:
        path = _find_config_path(override_path)
        _config_instance = Config(path)
    return _config_instance


def get_config() -> Config:
    if _config_instance is None:
        raise RuntimeError("Config not initialised — call init_config() first.")
    return _config_instance


def start_config_watcher(interval_seconds: int = 30) -> threading.Thread:
    def _loop() -> None:
        from fas.logger import get_logger
        logger = get_logger()
        while True:
            time.sleep(interval_seconds)
            try:
                if get_config().reload():
                    logger.info("config reloaded",
                                extra={"event": "config_reload", "status": "ok"})
            except Exception as exc:
                logger.warning(f"config reload failed: {exc}",
                               extra={"event": "config_reload", "status": "warn", "error": str(exc)})
    t = threading.Thread(target=_loop, name="config-watcher", daemon=True)
    t.start()
    return t
