"""Metric formulas, event repository and CSV export (no running cluster needed)."""
import csv
import io

from metrics import calculator as calc
from metrics.experiment_runner import SCENARIOS
from metrics.export_results import runs_csv, summary_csv
from storage.database import Database
from storage.event_repository import EventRepository


def nev(ts, etype, node=1, subject=None, **details):
    return {"event_id": f"{etype}-{ts}-{node}", "ts": ts, "node_id": node, "incarnation": 1, "event_type": etype,
            "epoch": 1, "subject_node_id": subject, "details": details}


def eev(ts, etype, node, election_id="e1", epoch=2, **kw):
    return {"event_id": f"{etype}-{ts}-{node}", "ts": ts, "node_id": node, "incarnation": 1, "event_type": etype,
            "epoch": epoch, "election_id": election_id, "duration_ms": None, "details": {}, **kw}


def test_detection_latency_is_declaration_minus_fault_time():
    evs = [nev(100.5, "NODE_SUSPECTED", 5, 2), nev(103.2, "NODE_FAILED", 5, 2), nev(103.9, "NODE_FAILED", 5, 2)]
    assert calc.detection_latency_ms(evs, target=2, fault_ts=100.0) == 3200.0
    assert calc.suspicion_latency_ms(evs, 2, 100.0) == 500.0
    assert calc.detection_latency_ms(evs, target=3, fault_ts=100.0) is None
    assert calc.detection_latency_ms(evs, target=2, fault_ts=104.0) is None       # declaration before the fault


def test_election_duration_and_failover():
    ee = [eev(10.0, "ELECTION_STARTED", 3, "e3"), eev(10.1, "ELECTION_STARTED", 4, "e4"),
          eev(10.3, "COORDINATOR_ANNOUNCED", 4, "e4", epoch=3), eev(10.6, "ELECTION_RETRY", 3, "e3")]
    s = calc.election_summary(ee, since=9.0)
    assert s["duration_ms"] == 300.0 and s["winner"] == 4 and s["epoch"] == 3
    assert s["elections_started"] == 2 and s["election_retries"] == 1
    assert calc.failover_time_ms(ee, fault_ts=9.0) == 1300.0


def test_recovery_time_uses_healthy_or_coordinator_announcement():
    nevs = [nev(52.0, "NODE_HEALTHY", 5, 2)]
    assert calc.recovery_time_ms(nevs, [], node=2, restart_ts=50.0) == 2000.0
    ee = [eev(51.2, "COORDINATOR_ANNOUNCED", 5)]
    assert calc.recovery_time_ms([], ee, node=5, restart_ts=50.0) == 1200.0


def test_false_positive_rate_uses_ground_truth():
    evs = [nev(10.0, "NODE_FAILED", 5, 2),            # node 2 really crashed at t=8
           nev(20.0, "NODE_FAILED", 5, 3),            # node 3 never crashed -> false positive
           nev(21.0, "COORDINATOR_TIMEOUT", 1, 5)]    # node 5 crashed at t=19.5 -> correct
    down = {2: [(8.0, None)], 5: [(19.5, None)]}
    fp = calc.false_positive_stats(evs, down, window_s=5.0, since=0.0)
    assert fp == {"total": 3, "incorrect": 1, "rate_pct": 100.0 / 3}
    assert calc.false_positive_stats([], down, 5.0, 0.0)["rate_pct"] == 0.0


def test_declaration_long_after_restart_is_incorrect():
    evs = [nev(100.0, "NODE_FAILED", 5, 2)]
    assert calc.false_positive_stats(evs, {2: [(8.0, 20.0)]}, window_s=5.0, since=0.0)["incorrect"] == 1


def test_monitoring_availability_is_time_weighted():
    samples = [(0.0, True, 5, 1), (1.0, True, 5, 1), (2.0, False, None, 1), (4.0, True, 4, 2), (5.0, True, 4, 2)]
    # up for [0,2) and [4,5) of 5 seconds
    assert calc.availability_pct(samples) == 60.0
    assert calc.availability_pct(samples[:1]) is None


def test_summarize_average_and_range():
    s = calc.summarize([10, 20, None, 30])
    assert (s["n"], s["mean"], s["min"], s["max"]) == (3, 20.0, 10.0, 30.0)
    assert calc.summarize([])["mean"] is None


def test_compute_run_metrics_end_to_end():
    events = {"node_events": [nev(5.0, "COORDINATOR_TIMEOUT", 1, 5), nev(7.5, "NODE_FAILED", 4, 5),
                              nev(9.0, "NODE_HEALTHY", 4, 5)],
              "election_events": [eev(5.2, "ELECTION_STARTED", 4), eev(5.3, "COORDINATOR_ANNOUNCED", 4, epoch=2)]}
    faults = [calc.GroundFault("crash", 5, start_ts=4.0, was_coordinator=True)]
    out = calc.compute_run_metrics(events=events, faults=faults, restarts=[(5, 8.0)],
                                   samples=[(4.0, True, 5, 1), (5.0, False, None, 1), (6.0, True, 4, 2)],
                                   overhead=(120, {"HEARTBEAT": 100, "ELECTION": 20}),
                                   down_intervals={5: [(4.0, 8.0)]}, since=3.0, until=10.0, failure_window_s=2.0)
    m = {k: v["value"] for k, v in out["metrics"].items()}
    assert m["detection_latency_ms"] == 1000.0
    assert m["election_duration_ms"] == 100.0 and m["failover_time_ms"] == 1300.0
    assert m["recovery_time_ms"] == 1000.0
    assert m["false_positive_rate_pct"] == 0.0 and m["message_overhead_msgs"] == 120
    assert m["monitoring_availability_pct"] == 50.0


def test_repository_roundtrip_and_epoch_recovery(tmp_path):
    repo = EventRepository(Database(tmp_path / "n.db"))
    repo.add_node_event(node_id=1, incarnation=1, event_type="X", epoch=3, details={"a": 1})
    repo.add_election_event(node_id=1, incarnation=1, event_type="ELECTION_WON", epoch=7, election_id="e")
    assert repo.max_epoch() == 7
    rows = repo.node_events()
    assert rows[0]["details"] == {"a": 1} and rows[0]["event_id"]
    # same event id twice is stored once (duplicates are identifiable when logs are collected)
    repo.add_node_event(node_id=1, incarnation=1, event_type="Y", event_id="dup")
    repo.add_node_event(node_id=1, incarnation=1, event_type="Y", event_id="dup")
    assert len(repo.node_events(event_types=["Y"])) == 1


def test_csv_reports(tmp_path):
    repo = EventRepository(Database(tmp_path / "c.db"))
    for run_i, v in enumerate([1000.0, 1400.0]):
        repo.add_metric_result(experiment_id="exp", run_index=run_i, scenario="coordinator_crash",
                               metric_name="detection_latency_ms", value=v, unit="ms")
    rows = repo.metric_results(experiment_id="exp")
    runs = list(csv.DictReader(io.StringIO(runs_csv(rows))))
    assert len(runs) == 2 and runs[0]["detection_latency_ms (ms)"] == "1000.0"
    summ = list(csv.DictReader(io.StringIO(summary_csv(rows))))[0]
    assert summ["average"] == "1200.0" and summ["min"] == "1000.0" and summ["max"] == "1400.0"


def test_all_eight_scenarios_are_registered():
    assert list(SCENARIOS) == ["initial_election", "worker_crash", "coordinator_crash", "coordinator_recovery",
                               "delayed_heartbeat", "message_loss", "repeated_failovers", "dashboard_resilience"]
    assert all(callable(s.run) and s.description for s in SCENARIOS.values())
