"""
watcher.py — Multi-folder filesystem watcher for FAS.

Spawns one watchdog observer per FolderProfile. Each observer feeds
FileTask items (file + profile) into a shared processing queue.
Falls back to a polling thread if watchdog is unavailable.
"""

import threading
import time
from pathlib import Path
from queue import Queue
from typing import Set

from fas.config_loader import FolderProfile, get_config
from fas.logger import log_event
from fas.processor import FileTask

try:
    from watchdog.events import FileSystemEventHandler, FileCreatedEvent
    from watchdog.observers import Observer
    from watchdog.observers.polling import PollingObserver
    _WATCHDOG = True
except ImportError:
    _WATCHDOG = False


# ---------------------------------------------------------------------------
# Depth helper
# ---------------------------------------------------------------------------

def _depth(file_path: Path, root: Path) -> int:
    try:
        return len(file_path.relative_to(root).parts)
    except ValueError:
        return 0


# ---------------------------------------------------------------------------
# Per-profile watchdog handler
# ---------------------------------------------------------------------------

if _WATCHDOG:
    class _ProfileHandler(FileSystemEventHandler):
        def __init__(self, profile: FolderProfile, queue: Queue, seen: Set[str]) -> None:
            super().__init__()
            self._profile = profile
            self._queue = queue
            self._seen = seen
            self._lock = threading.Lock()

        def on_created(self, event) -> None:
            if not event.is_directory:
                self._enqueue(Path(event.src_path))

        def on_moved(self, event) -> None:
            if not event.is_directory:
                self._enqueue(Path(event.dest_path))

        def _enqueue(self, path: Path) -> None:
            p = self._profile
            depth = _depth(path, p.watch_path)
            if depth == 0 or depth > p.watch_depth:
                return
            exts = p.watch_extensions
            if exts and path.suffix.lower() not in exts:
                return
            key = str(path.resolve())
            with self._lock:
                if key in self._seen:
                    return
                self._seen.add(key)
            time.sleep(0.3)   # let OS finish writing
            log_event("watch", status="enqueued", folder=p.name, file=str(path))
            self._queue.put(FileTask(path, p))


# ---------------------------------------------------------------------------
# WatcherPool — manages one observer per profile
# ---------------------------------------------------------------------------

class WatcherPool:
    """
    Starts one watcher per folder profile and routes events to a shared queue.
    """

    def __init__(self, queue: Queue, stop_event: threading.Event) -> None:
        self._queue = queue
        self._stop = stop_event
        self._observers: list = []
        self._poll_threads: list[threading.Thread] = []
        self._seen: Set[str] = set()
        self._seen_lock = threading.Lock()

    def start(self) -> None:
        cfg = get_config()
        for profile in cfg.profiles:
            if not profile.watch_path.exists():
                log_event("watch", status="warn", folder=profile.name,
                          error=f"Watch path missing: {profile.watch_path}")
                continue
            if _WATCHDOG:
                self._start_watchdog(profile)
            else:
                self._start_polling(profile)

    def stop(self) -> None:
        self._stop.set()
        for obs in self._observers:
            obs.stop()
        for obs in self._observers:
            obs.join(timeout=5)
        for t in self._poll_threads:
            t.join(timeout=5)

    # ------------------------------------------------------------------
    # Watchdog mode
    # ------------------------------------------------------------------

    def _start_watchdog(self, profile: FolderProfile) -> None:
        handler = _ProfileHandler(profile, self._queue, self._seen)
        try:
            obs = Observer()
        except Exception:
            obs = PollingObserver()  # type: ignore[assignment]
        obs.schedule(handler, str(profile.watch_path),
                     recursive=(profile.watch_depth > 1))
        obs.start()
        self._observers.append(obs)
        log_event("watch", status="started", folder=profile.name,
                  file=str(profile.watch_path))

    # ------------------------------------------------------------------
    # Polling fallback
    # ------------------------------------------------------------------

    def _start_polling(self, profile: FolderProfile) -> None:
        log_event("watch", status="polling", folder=profile.name,
                  file=str(profile.watch_path))
        t = threading.Thread(
            target=self._poll_loop, args=(profile,),
            name=f"poll-{profile.name}", daemon=True,
        )
        t.start()
        self._poll_threads.append(t)

    def _poll_loop(self, profile: FolderProfile) -> None:
        while not self._stop.is_set():
            try:
                for file_path in _iter_files(profile.watch_path, profile.watch_depth):
                    exts = profile.watch_extensions
                    if exts and file_path.suffix.lower() not in exts:
                        continue
                    key = str(file_path.resolve())
                    with self._seen_lock:
                        if key in self._seen:
                            continue
                        self._seen.add(key)
                    log_event("watch", status="enqueued", folder=profile.name,
                              file=str(file_path))
                    self._queue.put(FileTask(file_path, profile))
            except Exception as exc:
                log_event("watch", status="error", folder=profile.name, error=str(exc))
            time.sleep(5)


# ---------------------------------------------------------------------------
# Recursive file iterator with depth cap
# ---------------------------------------------------------------------------

def _iter_files(root: Path, max_depth: int):
    def _recurse(path: Path, depth: int):
        try:
            for entry in path.iterdir():
                if entry.is_file() and depth <= max_depth:
                    yield entry
                elif entry.is_dir() and depth < max_depth:
                    yield from _recurse(entry, depth + 1)
        except PermissionError:
            pass
    yield from _recurse(root, 1)
