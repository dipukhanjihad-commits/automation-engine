"""
processor.py — File processing pipeline for FAS.

Each file task carries the FolderProfile it belongs to.
Pipeline: insert DB record → validate → upload → update DB status.
A failure on one file never crashes the worker thread.
"""

import threading
import time
from pathlib import Path
from queue import Empty, Queue
from typing import NamedTuple, Optional

from fas.config_loader import FolderProfile
from fas.db import get_db, Status
from fas.logger import log_event
from fas.uploader import build_uploader, UploadResult


# ---------------------------------------------------------------------------
# Queue item: file + its owning profile
# ---------------------------------------------------------------------------

class FileTask(NamedTuple):
    file_path: Path
    profile: FolderProfile


# ---------------------------------------------------------------------------
# Processor worker
# ---------------------------------------------------------------------------

class FileProcessor:
    """
    Consumes FileTask items from a shared queue.
    Runs in a single daemon thread; processing is serial per queue but
    multiple processors can be spun up if throughput requires it.
    """

    def __init__(self, queue: Queue, stop_event: Optional[threading.Event] = None) -> None:
        self._queue = queue
        self._stop = stop_event or threading.Event()
        self._thread = threading.Thread(target=self._loop, name="file-processor", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        self._thread.join(timeout=timeout)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                task: FileTask = self._queue.get(timeout=1.0)
            except Empty:
                continue
            try:
                self._process(task)
            except Exception as exc:
                log_event("process", status="error",
                          folder=task.profile.name, file=str(task.file_path),
                          error=f"Unhandled: {exc}")
            finally:
                self._queue.task_done()

    # ------------------------------------------------------------------
    # Single-file pipeline
    # ------------------------------------------------------------------

    def _process(self, task: FileTask) -> None:
        profile = task.profile
        file_path = task.file_path
        db = get_db()
        t0 = time.perf_counter()

        # Guard: skip if already successfully sent (dedup across restarts)
        if db.is_already_sent(profile.table_name, str(file_path)):
            log_event("process", status="skip", folder=profile.name,
                      file=str(file_path), error="Already sent previously")
            return

        # 1. Validate
        if not self._validate(file_path, profile):
            return

        # 2. Gather metadata
        metadata = profile.metadata.copy()
        stat = file_path.stat()
        metadata["original_filename"] = file_path.name
        metadata["file_size_bytes"] = stat.st_size

        # 3. Insert DB record (status = pending)
        record_id = db.insert_file(
            table_name=profile.table_name,
            file_path=str(file_path),
            file_name=file_path.name,
            file_size=stat.st_size,
            metadata=metadata,
        )

        log_event("process", status="start", folder=profile.name, file=str(file_path))

        # 4. Upload
        uploader = build_uploader(profile)
        result: UploadResult = uploader.upload(file_path, metadata)
        latency_ms = (time.perf_counter() - t0) * 1000

        # 5. Update DB
        if result.success:
            db.mark_sent(profile.table_name, record_id)
            log_event("upload", status="sent", folder=profile.name,
                      file=str(file_path), latency_ms=latency_ms)
        else:
            db.mark_failed_attempt(
                table_name=profile.table_name,
                record_id=record_id,
                error=result.error or "unknown",
                retry_interval_seconds=profile.retry_interval,
                max_attempts=profile.retry_max_attempts,
            )
            log_event("upload", status="error", folder=profile.name,
                      file=str(file_path), attempt=1,
                      latency_ms=latency_ms, error=result.error)

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate(self, file_path: Path, profile: FolderProfile) -> bool:
        if not file_path.exists():
            log_event("validate", status="error", folder=profile.name,
                      file=str(file_path), error="File no longer exists")
            return False
        if not file_path.is_file():
            log_event("validate", status="error", folder=profile.name,
                      file=str(file_path), error="Not a regular file")
            return False
        exts = profile.watch_extensions
        if exts and file_path.suffix.lower() not in exts:
            log_event("validate", status="skip", folder=profile.name,
                      file=str(file_path), error=f"Extension {file_path.suffix!r} not allowed")
            return False
        return True
