"""Persist and query events. SQLite is a local event journal, not a replicated store."""
from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any, Callable, Dict, List, Optional

from storage.database import Database

log = logging.getLogger("dmp.events")


def new_id() -> str:
    return uuid.uuid4().hex


def _dumps(details: Optional[Dict[str, Any]]) -> Optional[str]:
    return json.dumps(details, default=str, separators=(",", ":")) if details else None


def _loads(row: dict) -> dict:
    d = row.get("details")
    row = dict(row)
    row["details"] = json.loads(d) if d else {}
    if "params" in row:
        row["params"] = json.loads(row["params"]) if row["params"] else {}
    return row


class EventRepository:
    """Low-level reads/writes for the four tables."""

    def __init__(self, db: Database):
        self.db = db

    # ------------------------------------------------------------------ writes
    def add_node_event(self, *, node_id: int, incarnation: int, event_type: str, epoch: int = 0,
                       subject_node_id: Optional[int] = None, details: Optional[dict] = None,
                       ts: Optional[float] = None, event_id: Optional[str] = None) -> str:
        eid = event_id or new_id()
        self.db.execute(
            "INSERT OR IGNORE INTO node_events (event_id, ts, node_id, incarnation, event_type, epoch, "
            "subject_node_id, details) VALUES (?,?,?,?,?,?,?,?)",
            (eid, ts or time.time(), node_id, incarnation, event_type, epoch, subject_node_id, _dumps(details)))
        return eid

    def add_election_event(self, *, node_id: int, incarnation: int, event_type: str, epoch: int = 0,
                           election_id: Optional[str] = None, duration_ms: Optional[float] = None,
                           details: Optional[dict] = None, ts: Optional[float] = None,
                           event_id: Optional[str] = None) -> str:
        eid = event_id or new_id()
        self.db.execute(
            "INSERT OR IGNORE INTO election_events (event_id, ts, node_id, incarnation, event_type, epoch, "
            "election_id, duration_ms, details) VALUES (?,?,?,?,?,?,?,?,?)",
            (eid, ts or time.time(), node_id, incarnation, event_type, epoch, election_id, duration_ms,
             _dumps(details)))
        return eid

    def start_fault(self, *, fault_type: str, target_node_id: Optional[int], params: Optional[dict] = None,
                    experiment_id: Optional[str] = None, run_index: Optional[int] = None,
                    scenario: Optional[str] = None, source: str = "node", start_ts: Optional[float] = None,
                    fault_id: Optional[str] = None) -> str:
        fid = fault_id or new_id()
        self.db.execute(
            "INSERT OR REPLACE INTO fault_experiments (fault_id, experiment_id, run_index, scenario, fault_type, "
            "target_node_id, params, start_ts, end_ts, source) VALUES (?,?,?,?,?,?,?,?,NULL,?)",
            (fid, experiment_id, run_index, scenario, fault_type, target_node_id, _dumps(params),
             start_ts or time.time(), source))
        return fid

    def end_fault(self, fault_id: str, end_ts: Optional[float] = None) -> None:
        self.db.execute("UPDATE fault_experiments SET end_ts=? WHERE fault_id=? AND end_ts IS NULL",
                        (end_ts or time.time(), fault_id))

    def end_open_faults(self, *, fault_type: str, target_node_id: int, end_ts: Optional[float] = None) -> None:
        self.db.execute(
            "UPDATE fault_experiments SET end_ts=? WHERE fault_type=? AND target_node_id=? AND end_ts IS NULL",
            (end_ts or time.time(), fault_type, target_node_id))

    def add_metric_result(self, *, experiment_id: str, run_index: int, scenario: str, metric_name: str,
                          value: Optional[float], unit: str = "", details: Optional[dict] = None,
                          ts: Optional[float] = None) -> str:
        rid = new_id()
        self.db.execute(
            "INSERT INTO metric_results (result_id, experiment_id, run_index, scenario, metric_name, value, unit, "
            "ts, details) VALUES (?,?,?,?,?,?,?,?,?)",
            (rid, experiment_id, run_index, scenario, metric_name, value, unit, ts or time.time(), _dumps(details)))
        return rid

    # ------------------------------------------------------------------ reads
    def node_events(self, *, since: Optional[float] = None, until: Optional[float] = None,
                    event_types: Optional[List[str]] = None, limit: int = 1000) -> List[dict]:
        return self._events("node_events", since, until, event_types, limit)

    def election_events(self, *, since: Optional[float] = None, until: Optional[float] = None,
                        event_types: Optional[List[str]] = None, limit: int = 1000) -> List[dict]:
        return self._events("election_events", since, until, event_types, limit)

    def _events(self, table: str, since, until, event_types, limit) -> List[dict]:
        where, params = [], []
        if since is not None:
            where.append("ts >= ?"); params.append(since)
        if until is not None:
            where.append("ts <= ?"); params.append(until)
        if event_types:
            where.append(f"event_type IN ({','.join('?' * len(event_types))})"); params.extend(event_types)
        clause = ("WHERE " + " AND ".join(where)) if where else ""
        # newest `limit` rows, returned in ascending order
        rows = self.db.query(f"SELECT * FROM (SELECT * FROM {table} {clause} ORDER BY ts DESC LIMIT ?) ORDER BY ts ASC",
                             (*params, int(limit)))
        return [_loads(r) for r in rows]

    def max_epoch(self) -> int:
        a = self.db.scalar("SELECT MAX(epoch) FROM node_events") or 0
        b = self.db.scalar("SELECT MAX(epoch) FROM election_events") or 0
        return int(max(a, b))

    def faults(self, *, since: Optional[float] = None, experiment_id: Optional[str] = None,
               limit: int = 500) -> List[dict]:
        where, params = [], []
        if since is not None:
            where.append("(start_ts >= ? OR end_ts IS NULL OR end_ts >= ?)"); params.extend([since, since])
        if experiment_id:
            where.append("experiment_id = ?"); params.append(experiment_id)
        clause = ("WHERE " + " AND ".join(where)) if where else ""
        rows = self.db.query(f"SELECT * FROM fault_experiments {clause} ORDER BY start_ts DESC LIMIT ?",
                             (*params, int(limit)))
        return [_loads(r) for r in rows]

    def metric_results(self, *, experiment_id: Optional[str] = None, limit: int = 20000) -> List[dict]:
        if experiment_id:
            rows = self.db.query("SELECT * FROM metric_results WHERE experiment_id=? ORDER BY run_index, ts",
                                 (experiment_id,))
        else:
            rows = self.db.query("SELECT * FROM metric_results ORDER BY ts DESC LIMIT ?", (limit,))
        return [_loads(r) for r in rows]

    def experiments(self) -> List[dict]:
        rows = self.db.query(
            "SELECT experiment_id, scenario, MIN(ts) AS started_ts, MAX(run_index) + 1 AS runs "
            "FROM metric_results GROUP BY experiment_id, scenario ORDER BY started_ts DESC")
        return rows


class EventLogger:
    """Binds a repository to one node identity so call sites stay short.

    Failures to persist are logged, never raised: monitoring must not die because of disk problems.
    """

    def __init__(self, repo: EventRepository, node_id: int, incarnation: int,
                 epoch_provider: Callable[[], int] = lambda: 0, name: str = "node"):
        self.repo, self.node_id, self.incarnation = repo, node_id, incarnation
        self._epoch = epoch_provider
        self._log = logging.getLogger(f"dmp.{name}")

    def node_event(self, event_type: str, subject: Optional[int] = None, *, level: int = logging.INFO,
                   quiet: bool = False, **details: Any) -> Optional[str]:
        if not quiet:
            self._log.log(level, "%s%s %s", event_type, f" (node {subject})" if subject else "",
                          details if details else "")
        try:
            return self.repo.add_node_event(node_id=self.node_id, incarnation=self.incarnation,
                                            event_type=event_type, epoch=self._epoch(),
                                            subject_node_id=subject, details=details or None)
        except Exception:  # pragma: no cover - defensive
            self._log.exception("failed to persist node event %s", event_type)
            return None

    def election_event(self, event_type: str, election_id: Optional[str] = None, *,
                       duration_ms: Optional[float] = None, epoch: Optional[int] = None,
                       **details: Any) -> Optional[str]:
        self._log.info("%s %s%s", event_type, election_id or "",
                       f" {duration_ms:.0f}ms" if duration_ms is not None else "")
        try:
            return self.repo.add_election_event(
                node_id=self.node_id, incarnation=self.incarnation, event_type=event_type,
                epoch=self._epoch() if epoch is None else epoch, election_id=election_id,
                duration_ms=duration_ms, details=details or None)
        except Exception:  # pragma: no cover - defensive
            self._log.exception("failed to persist election event %s", event_type)
            return None
