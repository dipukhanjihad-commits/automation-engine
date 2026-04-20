"""
db.py — SQLite persistence layer for FAS.

Design:
  - Single SQLite file shared by all folder profiles.
  - Each folder gets its OWN table (named after the folder).
  - Every file processed is recorded — sent successfully or failed.
  - status column: 'sent' | 'pending' | 'retrying' | 'failed'
  - Failed jobs stay in the table and are retried; on success status → 'sent'.

Table schema (created per folder, name = folder table_name):

    CREATE TABLE <folder> (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        file_path     TEXT    NOT NULL,
        file_name     TEXT    NOT NULL,
        file_size     INTEGER,
        metadata_json TEXT    NOT NULL DEFAULT '{}',
        attempt_count INTEGER NOT NULL DEFAULT 0,
        first_seen    TEXT    NOT NULL,
        last_attempt  TEXT,
        next_retry    TEXT,
        status        TEXT    NOT NULL DEFAULT 'pending',
        error         TEXT
    );
"""

import json
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional


# ---------------------------------------------------------------------------
# Status constants
# ---------------------------------------------------------------------------

class Status:
    PENDING   = "pending"
    RETRYING  = "retrying"
    SENT      = "sent"
    FAILED    = "failed"    # exhausted max attempts


# ---------------------------------------------------------------------------
# File record data class
# ---------------------------------------------------------------------------

@dataclass
class FileRecord:
    id: Optional[int]
    table_name: str
    file_path: str
    file_name: str
    file_size: Optional[int]
    metadata_json: str
    attempt_count: int
    first_seen: str
    last_attempt: Optional[str]
    next_retry: Optional[str]
    status: str
    error: Optional[str]

    @property
    def metadata(self) -> dict:
        try:
            return json.loads(self.metadata_json)
        except Exception:
            return {}


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

class Database:
    """
    Single SQLite file. One table per folder profile.
    Thread-safe via WAL mode + per-thread connections.
    """

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._local = threading.local()
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        # Prime the connection and enable WAL
        with self._cursor_raw() as cur:
            cur.execute("PRAGMA journal_mode=WAL")
            cur.execute("PRAGMA synchronous=NORMAL")

    # ------------------------------------------------------------------
    # Internal connection management
    # ------------------------------------------------------------------

    def _conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            self._local.conn = conn
        return self._local.conn

    @contextmanager
    def _cursor_raw(self) -> Generator[sqlite3.Cursor, None, None]:
        conn = self._conn()
        cur = conn.cursor()
        try:
            yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    def _future(self, seconds: int) -> str:
        return datetime.fromtimestamp(
            time.time() + seconds, tz=timezone.utc
        ).isoformat(timespec="seconds")

    def _row_to_record(self, row: sqlite3.Row, table_name: str) -> FileRecord:
        return FileRecord(
            id=row["id"],
            table_name=table_name,
            file_path=row["file_path"],
            file_name=row["file_name"],
            file_size=row["file_size"],
            metadata_json=row["metadata_json"],
            attempt_count=row["attempt_count"],
            first_seen=row["first_seen"],
            last_attempt=row["last_attempt"],
            next_retry=row["next_retry"],
            status=row["status"],
            error=row["error"],
        )

    # ------------------------------------------------------------------
    # Schema management — called once per folder profile at startup
    # ------------------------------------------------------------------

    def ensure_table(self, table_name: str) -> None:
        """Create the per-folder table if it doesn't already exist."""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS "{table_name}" (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path     TEXT    NOT NULL,
            file_name     TEXT    NOT NULL,
            file_size     INTEGER,
            metadata_json TEXT    NOT NULL DEFAULT '{{}}',
            attempt_count INTEGER NOT NULL DEFAULT 0,
            first_seen    TEXT    NOT NULL,
            last_attempt  TEXT,
            next_retry    TEXT,
            status        TEXT    NOT NULL DEFAULT 'pending',
            error         TEXT
        );
        CREATE INDEX IF NOT EXISTS "idx_{table_name}_status"
            ON "{table_name}" (status, next_retry);
        CREATE INDEX IF NOT EXISTS "idx_{table_name}_path"
            ON "{table_name}" (file_path);
        """
        with self._cursor_raw() as cur:
            cur.executescript(ddl)

    # ------------------------------------------------------------------
    # Insert — called when a file is first seen
    # ------------------------------------------------------------------

    def insert_file(
        self,
        table_name: str,
        file_path: str,
        file_name: str,
        file_size: Optional[int],
        metadata: dict,
    ) -> int:
        """
        Insert a new file record with status='pending'.
        Returns the new row id.
        """
        now = self._now()
        meta_json = json.dumps(metadata, ensure_ascii=False)
        with self._cursor_raw() as cur:
            cur.execute(
                f"""
                INSERT INTO "{table_name}"
                    (file_path, file_name, file_size, metadata_json,
                     attempt_count, first_seen, status)
                VALUES (?, ?, ?, ?, 0, ?, 'pending')
                """,
                (file_path, file_name, file_size, meta_json, now),
            )
            return cur.lastrowid  # type: ignore[return-value]

    def is_already_sent(self, table_name: str, file_path: str) -> bool:
        """Return True if this exact file_path already has status='sent'."""
        with self._cursor_raw() as cur:
            cur.execute(
                f'SELECT id FROM "{table_name}" WHERE file_path=? AND status="sent" LIMIT 1',
                (file_path,),
            )
            return cur.fetchone() is not None

    # ------------------------------------------------------------------
    # Status transitions
    # ------------------------------------------------------------------

    def mark_sent(self, table_name: str, record_id: int) -> None:
        now = self._now()
        with self._cursor_raw() as cur:
            cur.execute(
                f"""
                UPDATE "{table_name}"
                SET status='sent', last_attempt=?, attempt_count=attempt_count+1, error=NULL
                WHERE id=?
                """,
                (now, record_id),
            )

    def mark_failed_attempt(
        self,
        table_name: str,
        record_id: int,
        error: str,
        retry_interval_seconds: int,
        max_attempts: int,
    ) -> None:
        now = self._now()
        next_retry = self._future(retry_interval_seconds)
        with self._cursor_raw() as cur:
            cur.execute(
                f"""
                UPDATE "{table_name}"
                SET attempt_count  = attempt_count + 1,
                    last_attempt   = ?,
                    next_retry     = ?,
                    error          = ?,
                    status         = CASE
                        WHEN attempt_count + 1 >= ? THEN 'failed'
                        ELSE 'pending'
                    END
                WHERE id = ?
                """,
                (now, next_retry, error, max_attempts, record_id),
            )

    def mark_retrying(self, table_name: str, record_id: int) -> None:
        with self._cursor_raw() as cur:
            cur.execute(
                f'UPDATE "{table_name}" SET status="retrying" WHERE id=?',
                (record_id,),
            )

    # ------------------------------------------------------------------
    # Retry query — due jobs for a specific folder
    # ------------------------------------------------------------------

    def get_due_jobs(self, table_name: str, max_attempts: int) -> list[FileRecord]:
        now = self._now()
        with self._cursor_raw() as cur:
            cur.execute(
                f"""
                SELECT * FROM "{table_name}"
                WHERE status IN ('pending', 'retrying')
                  AND attempt_count < ?
                  AND (next_retry IS NULL OR next_retry <= ?)
                ORDER BY next_retry ASC
                """,
                (max_attempts, now),
            )
            return [self._row_to_record(r, table_name) for r in cur.fetchall()]

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def stats_for_table(self, table_name: str) -> dict:
        with self._cursor_raw() as cur:
            cur.execute(
                f'SELECT status, COUNT(*) as cnt FROM "{table_name}" GROUP BY status'
            )
            return {row["status"]: row["cnt"] for row in cur.fetchall()}

    def all_tables(self) -> list[str]:
        with self._cursor_raw() as cur:
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            return [row["name"] for row in cur.fetchall() if not row["name"].startswith("idx_")]

    def close(self) -> None:
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_db_instance: Optional[Database] = None
_db_lock = threading.Lock()


def init_db(db_path: str) -> Database:
    global _db_instance
    with _db_lock:
        _db_instance = Database(db_path)
    return _db_instance


def get_db() -> Database:
    if _db_instance is None:
        raise RuntimeError("DB not initialised — call init_db() first.")
    return _db_instance
