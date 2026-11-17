"""Experiment controller: ground truth, process control, fault requests and cluster observation.

The controller is a *client* of the cluster. It records the true fault times (it is the only
component that knows exactly when a process was killed), which is what makes detection latency
and false-positive measurements reliable. It writes to its own SQLite file (data/controller.db).
"""
from __future__ import annotations

import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional, Tuple

import httpx

from communication.messages import FaultControlMessage
from communication.peer_registry import PeerRegistry
from election.leader_discovery import STATE_OPERATIONAL, Discovery, discover_sync
from fault_injection import process_control
from fault_injection.heartbeat_faults import HEARTBEAT_DELAY, MESSAGE_LOSS
from node.config import ClusterConfig, load_config
from storage.database import Database
from storage.event_repository import EventRepository


class ClusterController:
    def __init__(self, config: Optional[ClusterConfig] = None):
        self.cfg = config or load_config()
        self.registry = PeerRegistry(self.cfg)
        self.http = httpx.Client(timeout=1.0)
        self.pool = ThreadPoolExecutor(max_workers=max(4, len(self.cfg.nodes) * 2))
        self.db = Database(self.cfg.controller_db_path)
        self.repo = EventRepository(self.db)

    def close(self) -> None:
        self.pool.shutdown(wait=False)
        self.http.close()
        self.db.close()

    # ================================================================== observation
    def discover(self, timeout: float = 0.8) -> Discovery:
        return discover_sync(self.registry, timeout=timeout, http=self.http, pool=self.pool)

    def _get(self, node_id: int, path: str, timeout: float = 1.0) -> Optional[dict]:
        try:
            r = self.http.get(self.registry.url(node_id, path), timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception:
            return None

    def node_status(self, node_id: int) -> Optional[dict]:
        return self._get(node_id, "/cluster/status")

    def coordinator_membership(self, disc: Optional[Discovery] = None) -> Tuple[Discovery, Optional[Dict[int, dict]]]:
        """Authoritative membership as seen by the current coordinator (None if there is none)."""
        disc = disc or self.discover()
        if disc.coordinator_id is None:
            return disc, None
        st = self.node_status(disc.coordinator_id)
        if not st or st.get("membership_source") != "authoritative":
            return disc, None
        return disc, {m["node_id"]: m for m in st["membership"]}

    def is_up(self, node_id: int) -> bool:
        return self._get(node_id, "/health", timeout=0.6) is not None

    def counters(self) -> Dict[int, dict]:
        results = list(self.pool.map(lambda n: (n.id, self._get(n.id, "/cluster/metrics", 0.8)), self.cfg.nodes))
        return {i: m for i, m in results if m}

    def collect_events(self, since: Optional[float] = None, until: Optional[float] = None,
                       limit: int = 20000) -> Dict[str, List[dict]]:
        """Collect journals from every reachable node and de-duplicate by event_id."""
        path = f"/cluster/events?limit={limit}" + (f"&since={since}" if since else "") + \
               (f"&until={until}" if until else "")
        results = list(self.pool.map(lambda n: self._get(n.id, path, 5.0), self.cfg.nodes))
        node_ev: Dict[str, dict] = {}
        elec_ev: Dict[str, dict] = {}
        for res in results:
            if not res:
                continue
            for e in res["node_events"]:
                node_ev[e["event_id"]] = e
            for e in res["election_events"]:
                elec_ev[e["event_id"]] = e
        return {"node_events": sorted(node_ev.values(), key=lambda e: e["ts"]),
                "election_events": sorted(elec_ev.values(), key=lambda e: e["ts"])}

    # ================================================================== process control
    def start_cluster(self, node_ids: Optional[List[int]] = None) -> Dict[int, float]:
        ts = {}
        for nid in node_ids or self.cfg.node_ids:
            ts[nid] = self.start(nid)
        return ts

    def start(self, node_id: int, *, experiment_id: Optional[str] = None, scenario: str = "manual",
              run_index: Optional[int] = None) -> float:
        """Start/restart a node process; returns the (ground-truth) restart timestamp."""
        ts = time.time()
        self.repo.end_open_faults(fault_type="crash", target_node_id=node_id, end_ts=ts)
        self.repo.start_fault(fault_type="restart", target_node_id=node_id, experiment_id=experiment_id,
                              scenario=scenario, run_index=run_index, source="controller", start_ts=ts)
        self.repo.db.execute("UPDATE fault_experiments SET end_ts=start_ts WHERE fault_type='restart' AND end_ts IS NULL")
        process_control.start_node(node_id, self.cfg)
        return ts

    def crash(self, node_id: int, *, experiment_id: Optional[str] = None, scenario: str = "manual",
              run_index: Optional[int] = None) -> float:
        """Hard-kill a node process. Returns the ground-truth fault timestamp."""
        disc = self.discover(timeout=0.5)
        was_coordinator = disc.coordinator_id == node_id
        ts = time.time()
        self.repo.start_fault(fault_type="crash", target_node_id=node_id,
                              params={"was_coordinator": was_coordinator, "epoch": disc.epoch},
                              experiment_id=experiment_id, scenario=scenario, run_index=run_index,
                              source="controller", start_ts=ts)
        process_control.stop_node(node_id, self.cfg)
        return ts

    # ================================================================== fault requests
    def _fault(self, node_id: int, msg: FaultControlMessage) -> bool:
        try:
            r = self.http.post(self.registry.url(node_id, "/faults"), json=msg.model_dump(mode="json"), timeout=2.0)
            return r.status_code == 200
        except Exception:
            return False

    def delay_heartbeats(self, node_id: int, delay_ms: float, duration_s: Optional[float] = None, *,
                         scope: str = "heartbeat", experiment_id: Optional[str] = None,
                         scenario: str = "manual", run_index: Optional[int] = None) -> Optional[float]:
        ts = time.time()
        ok = self._fault(node_id, FaultControlMessage(sender_id=0, incarnation=0, action="apply",
                                                     fault_type=HEARTBEAT_DELAY, delay_ms=delay_ms, scope=scope,
                                                     duration_s=duration_s, experiment_id=experiment_id))
        if ok:
            self._record_remote_fault(HEARTBEAT_DELAY, node_id, {"delay_ms": delay_ms, "scope": scope},
                                      duration_s, ts, experiment_id, scenario, run_index)
        return ts if ok else None

    def lose_messages(self, node_id: int, loss_pct: float, duration_s: Optional[float] = None, *,
                      scope: str = "heartbeat", experiment_id: Optional[str] = None,
                      scenario: str = "manual", run_index: Optional[int] = None) -> Optional[float]:
        ts = time.time()
        ok = self._fault(node_id, FaultControlMessage(sender_id=0, incarnation=0, action="apply",
                                                     fault_type=MESSAGE_LOSS, loss_pct=loss_pct, scope=scope,
                                                     duration_s=duration_s, experiment_id=experiment_id))
        if ok:
            self._record_remote_fault(MESSAGE_LOSS, node_id, {"loss_pct": loss_pct, "scope": scope},
                                      duration_s, ts, experiment_id, scenario, run_index)
        return ts if ok else None

    def clear_faults(self, node_id: Optional[int] = None) -> None:
        ids = [node_id] if node_id else self.cfg.node_ids
        now = time.time()
        for nid in ids:
            self._fault(nid, FaultControlMessage(sender_id=0, incarnation=0, action="clear"))
            for ft in (HEARTBEAT_DELAY, MESSAGE_LOSS):
                self.repo.end_open_faults(fault_type=ft, target_node_id=nid, end_ts=now)

    def _record_remote_fault(self, fault_type, node_id, params, duration_s, ts, experiment_id, scenario, run_index):
        fid = self.repo.start_fault(fault_type=fault_type, target_node_id=node_id,
                                    params={**params, "duration_s": duration_s}, experiment_id=experiment_id,
                                    scenario=scenario, run_index=run_index, source="controller", start_ts=ts)
        if duration_s:
            self.repo.end_fault(fid, ts + duration_s)

    # ================================================================== ground truth
    def down_intervals(self, since: Optional[float] = None) -> Dict[int, List[Tuple[float, Optional[float]]]]:
        out: Dict[int, List[Tuple[float, Optional[float]]]] = {}
        for row in self.repo.faults(since=since):
            if row["fault_type"] == "crash":
                out.setdefault(row["target_node_id"], []).append((row["start_ts"], row["end_ts"]))
        return out

    # ================================================================== waiting helpers
    def wait_until(self, predicate: Callable[[], Any], timeout: float, interval: float = 0.25) -> Tuple[bool, Any]:
        deadline = time.time() + timeout
        value = None
        while time.time() < deadline:
            value = predicate()
            if value:
                return True, value
            time.sleep(interval)
        return False, value

    def wait_for_coordinator(self, *, expected: Optional[int] = None, exclude: Optional[int] = None,
                             min_epoch: Optional[int] = None, timeout: float = 30.0) -> Optional[Discovery]:
        def check():
            d = self.discover()
            if d.state != STATE_OPERATIONAL or d.coordinator_id is None:
                return None
            if expected is not None and d.coordinator_id != expected:
                return None
            if exclude is not None and d.coordinator_id == exclude:
                return None
            if min_epoch is not None and d.epoch < min_epoch:
                return None
            return d
        ok, d = self.wait_until(check, timeout)
        return d if ok else None

    def is_stable(self) -> bool:
        """All nodes up, one coordinator (the highest priority), everyone agrees and is HEALTHY."""
        disc, members = self.coordinator_membership()
        if disc.state != STATE_OPERATIONAL or members is None:
            return False
        if len(disc.reachable) != len(self.cfg.nodes):
            return False
        top = max(self.cfg.nodes, key=lambda n: n.priority).id
        if disc.coordinator_id != top:
            return False
        for nid, h in disc.health.items():
            if not h or h.get("coordinator_id") != top or h.get("election_in_progress"):
                return False
        return all(m["health"] == "HEALTHY" for m in members.values())

    def wait_until_stable(self, timeout: float = 60.0) -> bool:
        ok, _ = self.wait_until(self.is_stable, timeout, interval=0.5)
        return ok

    def restore_cluster(self, timeout: float = 90.0, scenario: str = "restore") -> bool:
        """Start every stopped node, clear injected faults and wait for a stable cluster."""
        for nid in self.cfg.node_ids:
            if not self.is_up(nid):
                self.start(nid, scenario=scenario)
        self.clear_faults()
        return self.wait_until_stable(timeout)

    def new_experiment_id(self, scenario: str) -> str:
        return f"{scenario}-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:4]}"


class ClusterSampler(threading.Thread):
    """Background poller used during an experiment.

    Records whether monitoring was operational (a reachable coordinator existed) and keeps the
    latest message counters per (node, incarnation), so crashed processes still contribute.
    """

    def __init__(self, ctrl: ClusterController, interval: float = 0.4):
        super().__init__(daemon=True)
        self.ctrl, self.interval = ctrl, interval
        self.samples: List[Tuple[float, bool, Optional[int], int]] = []
        self.latest: Dict[Tuple[int, int], dict] = {}
        self.baseline: Dict[Tuple[int, int], int] = {}
        self._stop_evt = threading.Event()

    def begin(self) -> None:
        """Take the baseline synchronously, then start polling."""
        self.poll()
        self.baseline = {k: v["sent_total"] for k, v in self.latest.items()}
        self.samples.clear()
        self.start()

    def poll(self) -> None:
        d = self.ctrl.discover(timeout=0.6)
        self.samples.append((time.time(), d.state == STATE_OPERATIONAL, d.coordinator_id, d.epoch))
        for nid, m in self.ctrl.counters().items():
            self.latest[(nid, m["incarnation"])] = m["counters"]

    def run(self) -> None:
        while not self._stop_evt.is_set():
            try:
                self.poll()
            except Exception:
                pass
            self._stop_evt.wait(self.interval)

    def finish(self) -> None:
        self._stop_evt.set()
        if self.is_alive():
            self.join(timeout=5)
        try:
            self.poll()
        except Exception:
            pass

    def message_overhead(self) -> Tuple[int, Dict[str, int]]:
        total, by_type = 0, {}
        for key, c in self.latest.items():
            total += c["sent_total"] - self.baseline.get(key, 0)
            for t, n in c["sent"].items():
                by_type[t] = by_type.get(t, 0) + n
        return total, by_type
