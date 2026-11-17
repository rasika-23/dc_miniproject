"""Repeatable experiment scenarios.

Each scenario is a small function that drives the cluster through the controller (the only
component that knows the true fault times). After a scenario the runner collects the event
journals from every node, computes the metrics, stores them in controller.db and restores a
healthy cluster so the next repetition starts from the same state.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from election.leader_discovery import STATE_OPERATIONAL
from fault_injection import process_control
from fault_injection.controller import ClusterController, ClusterSampler
from metrics.calculator import GroundFault, compute_run_metrics, election_summary

Progress = Callable[[str, float], None]


# ============================================================================= context
@dataclass
class RunContext:
    ctrl: ClusterController
    scenario: str
    experiment_id: str
    run_index: int
    params: Dict[str, Any]
    defaults: Dict[str, Any]
    faults: List[GroundFault] = field(default_factory=list)
    restarts: List[Tuple[int, float]] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)
    checks: Dict[str, bool] = field(default_factory=dict)
    extra: Dict[str, Tuple[Optional[float], str]] = field(default_factory=dict)
    post_checks: List[Tuple[str, Callable[[Dict[str, list]], bool]]] = field(default_factory=list)
    t_begin: float = 0.0

    def p(self, name: str) -> Any:
        return self.params.get(name, self.defaults.get(name))

    def note(self, text: str) -> None:
        self.notes.append(text)

    def check(self, name: str, ok: bool) -> None:
        self.checks[name] = bool(ok)

    # ---- actions that also record ground truth
    def crash(self, node_id: int) -> float:
        was_coord = self.ctrl.discover(timeout=0.5).coordinator_id == node_id
        ts = self.ctrl.crash(node_id, experiment_id=self.experiment_id, scenario=self.scenario,
                             run_index=self.run_index)
        self.faults.append(GroundFault("crash", node_id, ts, was_coordinator=was_coord))
        return ts

    def restart(self, node_id: int) -> float:
        ts = self.ctrl.start(node_id, experiment_id=self.experiment_id, scenario=self.scenario,
                             run_index=self.run_index)
        self.restarts.append((node_id, ts))
        for f in self.faults:
            if f.fault_type == "crash" and f.target == node_id and f.end_ts is None:
                f.end_ts = ts
        return ts

    def delay(self, node_id: int, delay_ms: float, duration_s: float, scope: str = "heartbeat") -> Optional[float]:
        ts = self.ctrl.delay_heartbeats(node_id, delay_ms, duration_s, scope=scope, experiment_id=self.experiment_id,
                                        scenario=self.scenario, run_index=self.run_index)
        if ts:
            self.faults.append(GroundFault("heartbeat_delay", node_id, ts, ts + duration_s,
                                           params={"delay_ms": delay_ms, "scope": scope}))
        return ts

    def loss(self, node_id: int, pct: float, duration_s: float, scope: str = "heartbeat") -> Optional[float]:
        ts = self.ctrl.lose_messages(node_id, pct, duration_s, scope=scope, experiment_id=self.experiment_id,
                                     scenario=self.scenario, run_index=self.run_index)
        if ts:
            self.faults.append(GroundFault("message_loss", node_id, ts, ts + duration_s,
                                           params={"loss_pct": pct, "scope": scope}))
        return ts

    # ---- observations
    def coordinator(self) -> Optional[int]:
        return self.ctrl.discover().coordinator_id

    def member_health(self, node_id: int) -> Optional[str]:
        _, members = self.ctrl.coordinator_membership()
        return members[node_id]["health"] if members and node_id in members else None

    def wait_member(self, node_id: int, health: str, timeout: float) -> Optional[float]:
        ok, _ = self.ctrl.wait_until(lambda: self.member_health(node_id) == health, timeout)
        return time.time() if ok else None

    def pick_worker(self, preferred: int) -> int:
        coord = self.coordinator()
        if preferred != coord:
            return preferred
        others = [n for n in self.ctrl.cfg.nodes if n.id != coord]
        return min(others, key=lambda n: n.priority).id


# ============================================================================= scenarios
def _initial_prepare(ctx: RunContext) -> None:
    process_control.stop_all(ctx.ctrl.cfg)
    time.sleep(1.0)


def _initial_election(ctx: RunContext) -> None:
    top = max(ctx.ctrl.cfg.nodes, key=lambda n: n.priority).id
    t0 = time.time()
    ctx.ctrl.start_cluster()
    d = ctx.ctrl.wait_for_coordinator(timeout=90)
    ctx.extra["time_to_coordinator_ms"] = (round((time.time() - t0) * 1000, 1), "ms")
    ctx.check(f"node {top} (highest priority) became coordinator", bool(d and d.coordinator_id == top))
    stable = ctx.ctrl.wait_until_stable(60)
    ctx.extra["time_to_stable_ms"] = (round((time.time() - t0) * 1000, 1), "ms")
    ctx.check("all nodes agree and are HEALTHY", stable)


def _worker_crash(ctx: RunContext) -> None:
    target = ctx.pick_worker(int(ctx.p("target")))
    coord_before = ctx.coordinator()
    epoch_before = ctx.ctrl.discover().epoch
    t_fault = ctx.crash(target)
    failed_at = ctx.wait_member(target, "FAILED", ctx.ctrl.cfg.timing.failure_timeout + 20)
    ctx.check(f"coordinator declared node {target} FAILED", failed_at is not None)
    time.sleep(float(ctx.p("observe_s")))
    d = ctx.ctrl.discover()
    ctx.check("coordinator unchanged (no unnecessary election)",
              d.coordinator_id == coord_before and d.epoch == epoch_before)
    ctx.post_checks.append(("no election started after the worker crash",
                            lambda ev: not [e for e in ev["election_events"]
                                            if e["event_type"] == "ELECTION_STARTED" and e["ts"] >= t_fault]))
    ctx.restart(target)
    ok = ctx.wait_member(target, "HEALTHY", 45)
    ctx.check(f"node {target} returned to HEALTHY", ok is not None)


def _coordinator_crash(ctx: RunContext) -> None:
    old = ctx.coordinator()
    if old is None:
        raise RuntimeError("no coordinator to crash")
    others = [n for n in ctx.ctrl.cfg.nodes if n.id != old]
    expected = max(others, key=lambda n: n.priority).id
    ctx.crash(old)
    d = ctx.ctrl.wait_for_coordinator(exclude=old, timeout=45)
    ctx.check(f"node {expected} (highest alive) became coordinator", bool(d and d.coordinator_id == expected))
    failed = ctx.ctrl.wait_until(lambda: (ctx.member_health(old) == "FAILED"),
                                 ctx.ctrl.cfg.timing.failure_timeout + 15)[0]
    ctx.check(f"new coordinator declared node {old} FAILED", failed)


def _coordinator_recovery(ctx: RunContext) -> None:
    old = ctx.coordinator()
    if old is None:
        raise RuntimeError("no coordinator to crash")
    ctx.crash(old)
    d = ctx.ctrl.wait_for_coordinator(exclude=old, timeout=45)
    ctx.check("a new coordinator took over", d is not None)
    epoch_mid = d.epoch if d else 0
    time.sleep(1.0)
    t_restart = ctx.restart(old)
    back = ctx.ctrl.wait_for_coordinator(expected=old, timeout=60)
    ctx.check(f"node {old} reclaimed leadership via a new election", back is not None)
    ctx.check("epoch increased after the reclaim", bool(back and back.epoch > epoch_mid))
    ctx.check("cluster settled with all nodes HEALTHY", ctx.ctrl.wait_until_stable(45))
    ev = ctx.ctrl.collect_events(since=t_restart - 0.5)
    es = election_summary(ev["election_events"], t_restart - 0.5)
    ctx.extra["reclaim_election_duration_ms"] = (es["duration_ms"], "ms")


def _delayed_heartbeat(ctx: RunContext) -> None:
    target = ctx.pick_worker(int(ctx.p("target")))
    delay_ms, dur = float(ctx.p("delay_ms")), float(ctx.p("duration_s"))
    t = ctx.delay(target, delay_ms, dur)
    ctx.check("delay fault accepted by the node", t is not None)
    time.sleep(dur + delay_ms / 1000.0 + 2.0)
    ok = ctx.wait_member(target, "HEALTHY", 30)
    ctx.check(f"node {target} is HEALTHY again after the delay", ok is not None)
    ctx.note(f"delay {delay_ms:.0f} ms vs suspicion {ctx.ctrl.cfg.timing.suspicion_timeout:.1f}s / "
             f"failure {ctx.ctrl.cfg.timing.failure_timeout:.1f}s")
    ctx.post_checks.append(("no real crash happened, so every declaration is a false positive (informational)",
                            lambda ev: True))


def _message_loss(ctx: RunContext) -> None:
    target = ctx.pick_worker(int(ctx.p("target")))
    pct, dur = float(ctx.p("loss_pct")), float(ctx.p("duration_s"))
    ctx.check("heartbeat loss fault accepted", ctx.loss(target, pct, dur) is not None)
    time.sleep(dur + 2.0)
    ctx.check(f"node {target} HEALTHY after phase A", ctx.wait_member(target, "HEALTHY", 30) is not None)
    if not ctx.p("election_phase"):
        return
    old = ctx.coordinator()
    epct = float(ctx.p("election_loss_pct"))
    for n in ctx.ctrl.cfg.nodes:
        if n.id != old:
            ctx.loss(n.id, epct, 45.0, scope="election")
    ctx.crash(old)
    d = ctx.ctrl.wait_for_coordinator(exclude=old, timeout=60)
    ctx.check("a coordinator emerged despite election message loss", d is not None)
    time.sleep(4.0)
    ctx.ctrl.clear_faults()


def _repeated_failovers(ctx: RunContext) -> None:
    rounds = min(int(ctx.p("rounds")), len(ctx.ctrl.cfg.nodes) - 2)
    for i in range(rounds):
        old = ctx.coordinator()
        if old is None:
            ctx.check(f"round {i + 1}: a coordinator existed", False)
            return
        ctx.crash(old)
        d = ctx.ctrl.wait_for_coordinator(exclude=old, timeout=45)
        ctx.check(f"round {i + 1}: node {old} crashed, new coordinator elected",
                  bool(d and d.coordinator_id != old))
        time.sleep(float(ctx.p("pause_s")))


def _dashboard_resilience(ctx: RunContext) -> None:
    """Nothing polls the cluster while it works: monitoring must not depend on the dashboard."""
    quiet = float(ctx.p("quiet_s"))
    old = ctx.coordinator()
    ctx.note(f"no client polled the cluster for {quiet:.0f}s; then the coordinator was crashed")
    time.sleep(quiet)
    ctx.crash(old)
    t = ctx.ctrl.cfg.timing
    time.sleep(t.coordinator_timeout + t.election_timeout + t.failure_timeout + 4.0)
    d = ctx.ctrl.discover()
    ctx.check("a new coordinator was elected with no dashboard attached",
              d.state == STATE_OPERATIONAL and d.coordinator_id not in (None, old))
    ctx.post_checks.append(("failure of the old coordinator was logged by the surviving nodes",
                            lambda ev: any(e["event_type"] in ("NODE_FAILED", "COORDINATOR_TIMEOUT")
                                           and e.get("subject_node_id") == old for e in ev["node_events"])))


@dataclass
class Scenario:
    key: str
    title: str
    description: str
    params: Dict[str, Tuple[Any, str]]
    run: Callable[[RunContext], None]
    prepare: Optional[Callable[[RunContext], None]] = None
    uses_sampler: bool = True


SCENARIOS: Dict[str, Scenario] = {s.key: s for s in [
    Scenario("initial_election", "1. Initial election",
             "All nodes are stopped and started together. The highest-priority node should win the election.",
             {}, _initial_election, prepare=_initial_prepare),
    Scenario("worker_crash", "2. Worker crash",
             "A worker is killed. The coordinator must detect it (suspected, then failed) without starting a new election.",
             {"target": (2, "Worker node id to crash"), "observe_s": (3.0, "Seconds to watch for stray elections")},
             _worker_crash),
    Scenario("coordinator_crash", "3. Coordinator crash",
             "The coordinator is killed. Workers time out, run the Bully election and the next-highest node takes over.",
             {}, _coordinator_crash),
    Scenario("coordinator_recovery", "4. Coordinator recovery",
             "The crashed coordinator restarts and a new election decides whether it reclaims leadership.",
             {}, _coordinator_recovery),
    Scenario("delayed_heartbeat", "5. Delayed heartbeat",
             "A worker's heartbeats are delayed. Below the suspicion timeout nothing happens; above it you get false suspicions, "
             "and above the failure timeout a false failure that later recovers.",
             {"target": (2, "Worker node id"), "delay_ms": (6000, "Delay per heartbeat (ms)"),
              "duration_s": (15, "How long the delay is active (s)")}, _delayed_heartbeat),
    Scenario("message_loss", "6. Message loss",
             "Phase A drops a share of one worker's heartbeats (false suspicions). Phase B drops election messages while the "
             "coordinator crashes (election retries, conflicting claims).",
             {"target": (2, "Worker node id"), "loss_pct": (70, "Heartbeat loss (%)"),
              "duration_s": (20, "Phase A duration (s)"), "election_phase": (True, "Run phase B (election loss)"),
              "election_loss_pct": (60, "Election message loss (%)")}, _message_loss),
    Scenario("repeated_failovers", "7. Repeated failovers",
             "The coordinator is crashed several times in a row (5, then 4, then 3 ...). Each failover is timed.",
             {"rounds": (3, "Number of consecutive coordinator crashes"), "pause_s": (2.0, "Pause between rounds (s)")},
             _repeated_failovers),
    Scenario("dashboard_resilience", "8. Dashboard resilience",
             "No client polls the cluster during the test. The coordinator is crashed and the nodes must still elect a "
             "replacement on their own.",
             {"quiet_s": (8.0, "Quiet period before the crash (s)")}, _dashboard_resilience, uses_sampler=False),
]}


# ============================================================================= runner
@dataclass
class RunResult:
    scenario: str
    run_index: int
    experiment_id: str
    metrics: Dict[str, Dict[str, Any]]
    checks: Dict[str, bool]
    notes: List[str]
    details: Dict[str, Any]
    error: Optional[str] = None

    @property
    def passed(self) -> bool:
        return self.error is None and all(self.checks.values())


def _run_once(ctrl: ClusterController, sc: Scenario, experiment_id: str, run_index: int,
              params: Dict[str, Any], progress: Progress, frac: Tuple[float, float]) -> RunResult:
    lo, hi = frac
    defaults = {k: v[0] for k, v in sc.params.items()}
    ctx = RunContext(ctrl, sc.key, experiment_id, run_index, params, defaults)
    progress(f"Run {run_index + 1}: restoring a healthy cluster", lo)
    if not ctrl.restore_cluster():
        ctx.note("cluster was not fully stable before the run started")
    ctrl.repo.add_node_event(node_id=0, incarnation=0, event_type="EXPERIMENT_STARTED",
                             details={"scenario": sc.key, "experiment_id": experiment_id, "run": run_index + 1})
    if sc.prepare:
        sc.prepare(ctx)
    sampler = ClusterSampler(ctrl) if sc.uses_sampler else None
    ctx.t_begin = time.time()
    if sampler:
        sampler.begin()
    error = None
    progress(f"Run {run_index + 1}: running '{sc.title}'", lo + (hi - lo) * 0.15)
    try:
        sc.run(ctx)
    except Exception as exc:  # keep going so the cluster is restored and the failure is reported
        error = f"{type(exc).__name__}: {exc}"
    t_end = time.time()
    if sampler:
        sampler.finish()
    progress(f"Run {run_index + 1}: restoring cluster and collecting events", lo + (hi - lo) * 0.7)
    ctrl.clear_faults()
    ctrl.restore_cluster()
    events = ctrl.collect_events(since=ctx.t_begin - 1.0)
    window = {k: [e for e in v if e["ts"] <= t_end] for k, v in events.items()}
    for name, fn in ctx.post_checks:
        try:
            ctx.check(name, fn(window))
        except Exception:
            ctx.check(name, False)
    timing = ctrl.cfg.timing
    computed = compute_run_metrics(
        events=window, faults=ctx.faults, restarts=ctx.restarts,
        samples=sampler.samples if sampler else [], overhead=sampler.message_overhead() if sampler else (0, {}),
        down_intervals=ctrl.down_intervals(since=ctx.t_begin - 1.0), since=ctx.t_begin, until=t_end,
        failure_window_s=timing.failure_timeout + timing.coordinator_timeout)
    metrics = computed["metrics"]
    if not sampler:                       # nothing was polling, so these two would be meaningless zeros
        metrics.pop("message_overhead_msgs", None)
        metrics.pop("monitoring_availability_pct", None)
    if sc.key == "initial_election":      # the cluster is intentionally down at the start of this scenario
        metrics.pop("monitoring_availability_pct", None)
    for name, (value, unit) in ctx.extra.items():
        metrics[name] = {"value": value, "unit": unit}
    passed = sum(ctx.checks.values())
    metrics["checks_passed_pct"] = {"value": (100.0 * passed / len(ctx.checks)) if ctx.checks else None, "unit": "%"}
    details = {**computed["details"], "checks": ctx.checks, "notes": ctx.notes, "error": error}
    for name, m in metrics.items():
        ctrl.repo.add_metric_result(experiment_id=experiment_id, run_index=run_index, scenario=sc.key,
                                    metric_name=name, value=m["value"], unit=m["unit"],
                                    details=details if name == "checks_passed_pct" else None)
    return RunResult(sc.key, run_index, experiment_id, metrics, ctx.checks, ctx.notes, details, error)


def run_scenario(ctrl: ClusterController, key: str, repetitions: int = 1, params: Optional[Dict[str, Any]] = None,
                 progress: Optional[Progress] = None) -> List[RunResult]:
    """Run one scenario `repetitions` times; results are stored in controller.db."""
    sc = SCENARIOS[key]
    progress = progress or (lambda msg, frac: None)
    experiment_id = ctrl.new_experiment_id(key)
    results = []
    for i in range(repetitions):
        results.append(_run_once(ctrl, sc, experiment_id, i, params or {}, progress,
                                 (i / repetitions, (i + 1) / repetitions)))
    progress("Done", 1.0)
    return results
