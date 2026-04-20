"""
main.py — FAS (File Automation System) CLI entrypoint.

Usage:
    fas run              Start all folder watchers + retry daemons
    fas retry            Manual one-shot retry across all folders
    fas status           Print per-folder DB stats
    fas validate-config  Validate config.json and exit
"""

import argparse
import json
import signal
import sys
import threading
import time
from queue import Queue

import os
if getattr(sys, "frozen", False):
    sys.path.insert(0, sys._MEIPASS)  # type: ignore[attr-defined]

from fas.config_loader import init_config, get_config, start_config_watcher
from fas.db import init_db, get_db
from fas.logger import setup_logger, log_event, get_logger
from fas.processor import FileProcessor
from fas.retry_engine import RetryEngine, run_manual_retry
from fas.watcher import WatcherPool

_stop_event = threading.Event()


def _handle_signal(sig, frame) -> None:
    get_logger().info("Shutdown signal received",
                      extra={"event": "shutdown", "status": "ok"})
    _stop_event.set()


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_run(args) -> int:
    cfg = init_config(getattr(args, "config", None))
    setup_logger(cfg.log_level, cfg.log_file)

    # Create per-folder tables at startup
    db = init_db(cfg.db_path)
    for profile in cfg.profiles:
        db.ensure_table(profile.table_name)

    log_event("startup", status="ok",
              detail=f"{len(cfg.profiles)} folder(s) configured")

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    start_config_watcher(interval_seconds=30)

    queue: Queue = Queue()
    watcher_pool = WatcherPool(queue, _stop_event)
    processor = FileProcessor(queue, stop_event=_stop_event)
    retry_engine = RetryEngine(stop_event=_stop_event)

    watcher_pool.start()
    processor.start()
    retry_engine.start()

    log_event("run", status="ok", detail="All subsystems running — Ctrl-C to stop")

    try:
        while not _stop_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        _stop_event.set()

    log_event("shutdown", status="start")
    watcher_pool.stop()
    processor.stop()
    retry_engine.stop()
    log_event("shutdown", status="done")
    return 0


def cmd_retry(args) -> int:
    cfg = init_config(getattr(args, "config", None))
    setup_logger(cfg.log_level, cfg.log_file)
    db = init_db(cfg.db_path)
    for profile in cfg.profiles:
        db.ensure_table(profile.table_name)
    run_manual_retry()
    return 0


def cmd_status(args) -> int:
    cfg = init_config(getattr(args, "config", None))
    setup_logger("WARNING")
    db = init_db(cfg.db_path)
    for profile in cfg.profiles:
        db.ensure_table(profile.table_name)

    report = {
        "config_path": str(cfg._path),
        "database": cfg.db_path,
        "folders": [],
    }
    for profile in cfg.profiles:
        stats = db.stats_for_table(profile.table_name)
        report["folders"].append({
            "name": profile.name,
            "table": profile.table_name,
            "watch_path": str(profile.watch_path),
            "upload_type": profile.upload_type,
            "stats": stats,
        })

    print(json.dumps(report, indent=2))
    return 0


def cmd_validate_config(args) -> int:
    try:
        cfg = init_config(getattr(args, "config", None))
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"ERROR parsing config: {exc}", file=sys.stderr)
        return 1

    errors = cfg.validate()
    if errors:
        print("Config validation FAILED:")
        for e in errors:
            print(f"  - {e}")
        return 1

    print(f"Config OK — {cfg._path}")
    print(f"  Database : {cfg.db_path}")
    print(f"  Folders  : {len(cfg.profiles)}")
    for p in cfg.profiles:
        ext_str = ", ".join(p.watch_extensions) if p.watch_extensions else "all"
        print(f"\n  [{p.name}]  →  table: {p.table_name}")
        print(f"    watch path  : {p.watch_path}")
        print(f"    depth       : {p.watch_depth}")
        print(f"    extensions  : {ext_str}")
        print(f"    upload      : {p.upload_type.upper()}")
        print(f"    retry       : every {p.retry_interval}s, max {p.retry_max_attempts} attempts")
    return 0


# ---------------------------------------------------------------------------
# CLI parser
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fas", description="FAS — File Automation System")
    parser.add_argument("--config", metavar="PATH", default=None,
                        help="Override path to config.json")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("run",             help="Start all watchers and retry daemons")
    sub.add_parser("retry",           help="Run a manual retry cycle across all folders")
    sub.add_parser("status",          help="Print per-folder DB stats as JSON")
    sub.add_parser("validate-config", help="Validate config.json and exit")
    return parser

def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    handlers = {
        "run": cmd_run,
        "retry": cmd_retry,
        "status": cmd_status,
        "validate-config": cmd_validate_config,
    }

    command = args.command or "run"   # 👈 default behavior

    sys.exit(handlers[command](args))


if __name__ == "__main__":
    main()
