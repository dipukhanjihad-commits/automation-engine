"""
retry_engine.py — Background retry engine for FAS.

Runs one retry loop per FolderProfile (different intervals/max_attempts).
All retry loops share the same Database instance but operate on their
own table so they never interfere.
"""

import threading
import time
from pathlib import Path

from fas.config_loader import FolderProfile, get_config
from fas.db import FileRecord, get_db, Status
from fas.logger import log_event
from fas.uploader import UploadResult, build_uploader


class _ProfileRetryLoop:
    """Retry daemon for a single folder profile."""

    def __init__(self, profile: FolderProfile, stop_event: threading.Event) -> None:
        self._profile = profile
        self._stop = stop_event
        self._thread = threading.Thread(
            target=self._loop,
            name=f"retry-{profile.name}",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._cycle()
            except Exception as exc:
                log_event("retry", status="error", folder=self._profile.name,
                          error=f"Cycle error: {exc}")
            # Sleep in 1-second ticks so stop_event is noticed promptly
            for _ in range(self._profile.retry_interval):
                if self._stop.is_set():
                    break
                time.sleep(1)

    def _cycle(self) -> None:
        p = self._profile
        db = get_db()
        due = db.get_due_jobs(p.table_name, p.retry_max_attempts)
        if not due:
            return
        log_event("retry", status="cycle", folder=p.name,
                  detail=f"{len(due)} jobs due")
        uploader = build_uploader(p)
        for record in due:
            if self._stop.is_set():
                break
            self._attempt(record, uploader)

    def _attempt(self, record: FileRecord, uploader) -> None:
        p = self._profile
        db = get_db()
        file_path = Path(record.file_path)
        attempt = record.attempt_count + 1

        log_event("retry", status="attempt", folder=p.name,
                  file=str(file_path), attempt=attempt)
        db.mark_retrying(p.table_name, record.id)  # type: ignore[arg-type]

        if not file_path.exists():
            db.mark_failed_attempt(p.table_name, record.id,  # type: ignore[arg-type]
                                   "File not found",
                                   p.retry_interval, p.retry_max_attempts)
            log_event("retry", status="error", folder=p.name,
                      file=str(file_path), attempt=attempt, error="File not found")
            return

        t0 = time.perf_counter()
        result: UploadResult = uploader.upload(file_path, record.metadata)
        latency_ms = (time.perf_counter() - t0) * 1000

        if result.success:
            db.mark_sent(p.table_name, record.id)  # type: ignore[arg-type]
            log_event("retry", status="sent", folder=p.name,
                      file=str(file_path), attempt=attempt, latency_ms=latency_ms)
        else:
            db.mark_failed_attempt(p.table_name, record.id,  # type: ignore[arg-type]
                                   result.error or "unknown",
                                   p.retry_interval, p.retry_max_attempts)
            log_event("retry", status="error", folder=p.name,
                      file=str(file_path), attempt=attempt,
                      latency_ms=latency_ms, error=result.error)


# ---------------------------------------------------------------------------
# Engine: manages one loop per profile
# ---------------------------------------------------------------------------

class RetryEngine:
    def __init__(self, stop_event: threading.Event | None = None) -> None:
        self._stop = stop_event or threading.Event()
        self._loops: list[_ProfileRetryLoop] = []

    def start(self) -> None:
        cfg = get_config()
        for profile in cfg.profiles:
            loop = _ProfileRetryLoop(profile, self._stop)
            loop.start()
            self._loops.append(loop)
        log_event("retry", status="started",
                  detail=f"{len(self._loops)} profile(s)")

    def stop(self) -> None:
        self._stop.set()
        log_event("retry", status="stopped")


# ---------------------------------------------------------------------------
# Manual one-shot (fas retry CLI command)
# ---------------------------------------------------------------------------

def run_manual_retry() -> None:
    log_event("retry", status="manual_start")
    cfg = get_config()
    db = get_db()
    for profile in cfg.profiles:
        uploader = build_uploader(profile)
        due = db.get_due_jobs(profile.table_name, profile.retry_max_attempts)
        log_event("retry", status="manual_cycle", folder=profile.name,
                  detail=f"{len(due)} jobs due")
        loop = _ProfileRetryLoop(profile, threading.Event())
        for record in due:
            loop._attempt(record, uploader)
    log_event("retry", status="manual_done")
