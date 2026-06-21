"""SQLite-backed idempotency and audit state."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from health_sync.mappings import canonical_json

TERMINAL_CLEANUP_STATUSES = ("success", "skipped", "dry_run", "resolved")


def utc_now() -> datetime:
    return datetime.now(UTC)


def utc_iso(now: datetime | None = None) -> str:
    return (now or utc_now()).isoformat()


@dataclass(frozen=True)
class CleanupResult:
    sync_events_deleted: int
    failed_events_deleted: int
    sync_runs_deleted: int
    dead_letters_deleted: int
    checkpoint_done: bool
    vacuum_done: bool
    analyze_done: bool
    db_size_bytes: int


class SyncState:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.init_db()

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("PRAGMA journal_mode = WAL")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def init_db(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS sync_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    flow_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    dry_run INTEGER NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    error TEXT
                );

                CREATE TABLE IF NOT EXISTS sync_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER,
                    flow_name TEXT NOT NULL,
                    source TEXT NOT NULL,
                    target TEXT NOT NULL,
                    logical_key TEXT NOT NULL,
                    source_timestamp TEXT,
                    payload_hash TEXT NOT NULL,
                    status TEXT NOT NULL,
                    dry_run INTEGER NOT NULL,
                    message TEXT,
                    error TEXT,
                    payload_json TEXT,
                    created_at TEXT NOT NULL,
                    applied_at TEXT,
                    FOREIGN KEY(run_id) REFERENCES sync_runs(id)
                );

                CREATE INDEX IF NOT EXISTS idx_sync_events_cleanup
                ON sync_events(status, created_at);

                CREATE INDEX IF NOT EXISTS idx_sync_events_key
                ON sync_events(logical_key, target, payload_hash);

                CREATE TABLE IF NOT EXISTS sync_state (
                    logical_key TEXT NOT NULL,
                    target TEXT NOT NULL,
                    source TEXT NOT NULL,
                    payload_hash TEXT NOT NULL,
                    source_timestamp TEXT,
                    payload_json TEXT,
                    applied_at TEXT NOT NULL,
                    last_event_id INTEGER,
                    PRIMARY KEY (logical_key, target)
                );

                CREATE TABLE IF NOT EXISTS dead_letters (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER,
                    flow_name TEXT NOT NULL,
                    source TEXT NOT NULL,
                    target TEXT NOT NULL,
                    logical_key TEXT NOT NULL,
                    payload_hash TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    resolved_at TEXT,
                    FOREIGN KEY(event_id) REFERENCES sync_events(id)
                );

                CREATE INDEX IF NOT EXISTS idx_dead_letters_cleanup
                ON dead_letters(status, resolved_at);
                """
            )

    def begin_run(self, flow_name: str, dry_run: bool) -> int:
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO sync_runs(flow_name, status, dry_run, started_at)
                VALUES (?, 'running', ?, ?)
                """,
                (flow_name, int(dry_run), utc_iso()),
            )
            return int(cursor.lastrowid)

    def finish_run(self, run_id: int, status: str, error: str | None = None) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE sync_runs
                SET status = ?, finished_at = ?, error = ?
                WHERE id = ?
                """,
                (status, utc_iso(), error, run_id),
            )

    def current_hash(self, logical_key: str, target: str) -> str | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT payload_hash FROM sync_state
                WHERE logical_key = ? AND target = ?
                """,
                (logical_key, target),
            ).fetchone()
            return str(row["payload_hash"]) if row else None

    def current_payload(self, logical_key: str, target: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT payload_json FROM sync_state
                WHERE logical_key = ? AND target = ?
                """,
                (logical_key, target),
            ).fetchone()
            if not row or not row["payload_json"]:
                return None
            import json

            return json.loads(row["payload_json"])

    def already_applied(self, logical_key: str, target: str, payload_hash_value: str) -> bool:
        return self.current_hash(logical_key, target) == payload_hash_value

    def record_event(
        self,
        *,
        run_id: int | None,
        flow_name: str,
        source: str,
        target: str,
        logical_key: str,
        payload_hash_value: str,
        status: str,
        dry_run: bool,
        payload: dict[str, Any] | None = None,
        source_timestamp: str | None = None,
        message: str | None = None,
        error: str | None = None,
        update_state: bool = False,
    ) -> int:
        payload_json = canonical_json(payload) if payload is not None else None
        applied_at = utc_iso() if status == "success" else None
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO sync_events(
                    run_id, flow_name, source, target, logical_key, source_timestamp,
                    payload_hash, status, dry_run, message, error, payload_json,
                    created_at, applied_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    flow_name,
                    source,
                    target,
                    logical_key,
                    source_timestamp,
                    payload_hash_value,
                    status,
                    int(dry_run),
                    message,
                    error,
                    payload_json,
                    utc_iso(),
                    applied_at,
                ),
            )
            event_id = int(cursor.lastrowid)
            if update_state and status == "success" and not dry_run:
                conn.execute(
                    """
                    INSERT INTO sync_state(
                        logical_key, target, source, payload_hash, source_timestamp,
                        payload_json, applied_at, last_event_id
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(logical_key, target) DO UPDATE SET
                        source = excluded.source,
                        payload_hash = excluded.payload_hash,
                        source_timestamp = excluded.source_timestamp,
                        payload_json = excluded.payload_json,
                        applied_at = excluded.applied_at,
                        last_event_id = excluded.last_event_id
                    """,
                    (
                        logical_key,
                        target,
                        source,
                        payload_hash_value,
                        source_timestamp,
                        payload_json,
                        applied_at,
                        event_id,
                    ),
                )
            if status == "failed":
                conn.execute(
                    """
                    INSERT INTO dead_letters(
                        event_id, flow_name, source, target, logical_key, payload_hash,
                        status, error, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, 'unresolved', ?, ?)
                    """,
                    (
                        event_id,
                        flow_name,
                        source,
                        target,
                        logical_key,
                        payload_hash_value,
                        error or "Unknown error",
                        utc_iso(),
                    ),
                )
            return event_id

    def cleanup(
        self,
        *,
        now: datetime | None = None,
        successful_event_retention_days: int = 90,
        failed_event_retention_days: int = 180,
        run_retention_days: int = 90,
        resolved_dead_letter_retention_days: int = 180,
        vacuum: bool = False,
        analyze: bool = False,
    ) -> CleanupResult:
        now = now or utc_now()
        success_cutoff = (now - timedelta(days=successful_event_retention_days)).isoformat()
        failed_cutoff = (now - timedelta(days=failed_event_retention_days)).isoformat()
        run_cutoff = (now - timedelta(days=run_retention_days)).isoformat()
        dead_letter_cutoff = (now - timedelta(days=resolved_dead_letter_retention_days)).isoformat()

        with self.connect() as conn:
            cursor = conn.execute(
                """
                DELETE FROM sync_events
                WHERE status IN ('success', 'skipped', 'dry_run')
                  AND created_at < ?
                """,
                (success_cutoff,),
            )
            sync_events_deleted = cursor.rowcount

            cursor = conn.execute(
                """
                DELETE FROM sync_events
                WHERE status = 'failed'
                  AND created_at < ?
                  AND id NOT IN (
                    SELECT event_id FROM dead_letters
                    WHERE event_id IS NOT NULL AND status != 'resolved'
                  )
                """,
                (failed_cutoff,),
            )
            failed_events_deleted = cursor.rowcount

            cursor = conn.execute(
                """
                DELETE FROM sync_runs
                WHERE status NOT IN ('pending', 'running', 'retrying')
                  AND COALESCE(finished_at, started_at) < ?
                """,
                (run_cutoff,),
            )
            sync_runs_deleted = cursor.rowcount

            cursor = conn.execute(
                """
                DELETE FROM dead_letters
                WHERE status = 'resolved'
                  AND resolved_at IS NOT NULL
                  AND resolved_at < ?
                """,
                (dead_letter_cutoff,),
            )
            dead_letters_deleted = cursor.rowcount

        with sqlite3.connect(self.path) as conn:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            checkpoint_done = True
            if vacuum:
                conn.execute("VACUUM")
            if analyze:
                conn.execute("ANALYZE")

        return CleanupResult(
            sync_events_deleted=sync_events_deleted,
            failed_events_deleted=failed_events_deleted,
            sync_runs_deleted=sync_runs_deleted,
            dead_letters_deleted=dead_letters_deleted,
            checkpoint_done=checkpoint_done,
            vacuum_done=vacuum,
            analyze_done=analyze,
            db_size_bytes=self.path.stat().st_size if self.path.exists() else 0,
        )
