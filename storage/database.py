"""SQLite connection handling. Each node process owns one local database file."""
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence

SCHEMA_PATH = Path(__file__).with_name("schema.sql")


class Database:
    """Small thread-safe wrapper around one SQLite file (WAL mode)."""

    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False, timeout=10)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
            self._conn.commit()

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        with self._lock:
            self._conn.execute(sql, params)
            self._conn.commit()

    def executemany(self, sql: str, rows: Iterable[Sequence[Any]]) -> None:
        with self._lock:
            self._conn.executemany(sql, list(rows))
            self._conn.commit()

    def query(self, sql: str, params: Sequence[Any] = ()) -> List[dict]:
        with self._lock:
            cur = self._conn.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]

    def scalar(self, sql: str, params: Sequence[Any] = ()) -> Optional[Any]:
        with self._lock:
            row = self._conn.execute(sql, params).fetchone()
            return None if row is None else row[0]

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.close()
            except sqlite3.Error:
                pass
