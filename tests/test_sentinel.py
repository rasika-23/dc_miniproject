"""Sentinel: service monitoring on top of the cluster (assignment, states, incidents, alerts, failover)."""
import time

from metrics import calculator as calc
from node.config import SentinelSettings, ServiceSpec
from sentinel.assignment import assign, diff, probers_of
from sentinel.service_state import DEGRADED, DOWN, NO_DATA, UNKNOWN, UP, ServiceTracker, availability_pct
from sentinel.views import view_from_snapshot
from tests.conftest import run, running_sentinel, wait_until

SPEC = ServiceSpec(id="pay", name="Payments", url="http://x/health", interval_s=1.0, latency_warn_ms=100,
                   criticality="critical", slo_target=99.9)
SET = SentinelSettings(failure_threshold=3, recovery_threshold=2, degraded_threshold=3, quorum=2)


def res(node, ts, ok=True, lat=10.0, err=None):
    return {"service_id": "pay", "node_id": node, "ts": ts, "ok": ok, "latency_ms": lat, "error": err}


# ----------------------------------------------------------------------------- pure logic
def test_assignment_is_balanced_replicated_and_sticky():
    svc = ["a", "b", "c", "d"]
    a = assign(svc, [1, 2, 3, 4, 5], 2)
    assert all(len(probers_of(a, s)) == 2 for s in svc)
    loads = [len(v) for v in a.values()]
    assert max(loads) - min(loads) <= 1
    b = assign(svc, [1, 2, 3, 5], 2, previous=a)                      # node 4 leaves
    assert all(4 not in probers_of(b, s) for s in svc)
    assert all(len(probers_of(b, s)) == 2 for s in svc)
    moved = diff(a, b)
    assert set(moved) == {s for s in svc if 4 in probers_of(a, s)}      # only services node 4 probed moved
    assert assign(svc, [3], 2) == {3: svc}                              # one node left probes everything


def test_down_needs_consecutive_failures_and_quorum_of_probers():
    t = ServiceTracker(SPEC, SET, now=0.0)
    t.probers = [1, 2]
    assert [x.new for x in t.ingest(res(1, 1.0))] == [UP]
    for i, ts in enumerate([2.0, 3.0, 4.0, 5.0]):                       # four failures, all from node 1
        assert t.ingest(res(1, ts, ok=False, err="HTTP 503")) == []
    assert t.status == UP                                               # one node alone cannot declare an outage
    moves = t.ingest(res(2, 6.0, ok=False, err="HTTP 503"))
    assert [x.new for x in moves] == [DOWN] and "nodes [1, 2]" in moves[0].reason
    assert t.down_periods == [[6.0, None]]
    assert t.ingest(res(1, 7.0)) == []                                  # recovery needs 2 good checks
    assert [x.new for x in t.ingest(res(2, 8.0))] == [UP]
    assert t.down_periods == [[6.0, 8.0]]


def test_quorum_is_capped_by_live_probers():
    t = ServiceTracker(SPEC, SET, now=0.0)
    t.probers = [3]                                                     # the other prober died
    t.ingest(res(3, 1.0))
    out = []
    for ts in (2.0, 3.0, 4.0):
        out += t.ingest(res(3, ts, ok=False))
    assert [x.new for x in out] == [DOWN]


def test_slow_checks_degrade_and_recover_and_replays_are_ignored():
    t = ServiceTracker(SPEC, SET, now=0.0)
    t.probers = [1]
    t.ingest(res(1, 1.0))
    out = []
    for ts in (2.0, 3.0, 4.0):
        out += t.ingest(res(1, ts, lat=500))
    assert [x.new for x in out] == [DEGRADED]
    assert t.ingest(res(1, 4.0, lat=500)) == []                         # same (node, ts): ignored
    t.ingest(res(1, 5.0)); out = t.ingest(res(1, 6.0))
    assert [x.new for x in out] == [UP]


def test_no_data_when_checks_stop_but_not_inside_the_grace_period():
    t = ServiceTracker(SPEC, SET, now=0.0)
    t.probers = [1]
    t.ingest(res(1, 10.0))
    assert t.check_no_data(now=12.0) == []                              # 2 s < 4 x interval
    assert t.check_no_data(now=20.0, grace_until=30.0) == []            # inside takeover grace
    assert [x.new for x in t.check_no_data(now=20.0)] == [NO_DATA]
    assert [x.new for x in t.ingest(res(1, 21.0))] == [UP]


def test_tracker_roundtrips_through_a_snapshot():
    t = ServiceTracker(SPEC, SET, now=0.0)
    t.probers = [1, 2]
    t.ingest(res(1, 1.0))
    for ts, n in ((2.0, 1), (3.0, 2), (4.0, 1)):
        t.ingest(res(n, ts, ok=False, err="timeout"))
    assert t.status == DOWN
    t2 = ServiceTracker(SPEC, SET, now=100.0)
    t2.restore(t.to_dict())
    assert t2.status == DOWN and t2.down_periods == t.down_periods and t2.last_error == "timeout"


def test_availability_and_error_budget_view():
    assert availability_pct([[10.0, 20.0]], 0.0, 100.0) == 90.0
    assert availability_pct([[90.0, None]], 0.0, 100.0) == 90.0
    snap = {"generated_at": 100.0, "coordinator": 5, "epoch": 3, "assignments": {"1": ["pay"], "2": ["pay"]},
            "services": {"pay": {"status": "UP", "first_seen": 0.0, "down_periods": [[10.0, 18.64]], "probers": [1, 2]}},
            "incidents": [{"incident_id": "I1", "service_id": "pay", "state": "resolved", "opened_ts": 10.0,
                           "resolved_ts": 20.0}]}
    v = view_from_snapshot(snap, [SPEC], window_h=24.0, now=100.0)
    sv = v["services"][0]
    assert v["summary"]["UP"] == 1 and v["summary"]["mttr_s"] == 10.0 and v["summary"]["coverage_pct"] == 100.0
    assert sv["probers"] == [1, 2] and abs(sv["availability_pct"] - 91.36) < 0.01
    assert sv["error_budget_left_pct"] is not None and sv["error_budget_left_pct"] < 1e-6 + 100  # 99.9% SLO of 24 h


def test_sentinel_metric_formulas():
    ev = [{"event_type": "INCIDENT_OPENED", "ts": 12.0, "node_id": 5, "details": {"service": "pay", "incident_id": "I1"}},
          {"event_type": "ALERT_DISPATCHED", "ts": 12.5, "node_id": 5,
           "details": {"service": "pay", "event": "opened", "dedupe_key": "I1:opened"}},
          {"event_type": "ALERT_DISPATCHED", "ts": 15.5, "node_id": 4,
           "details": {"service": "pay", "event": "opened", "dedupe_key": "I1:opened"}},
          {"event_type": "INCIDENT_RESOLVED", "ts": 31.0, "node_id": 4, "details": {"service": "pay"}},
          {"event_type": "ASSIGNMENT_REBALANCED", "ts": 46.0, "node_id": 5, "details": {}}]
    assert calc.service_detection_latency_ms(ev, "pay", 10.0) == 2000.0
    assert calc.alert_latency_ms(ev, "pay", 10.0) == 2500.0
    assert calc.resolution_latency_ms(ev, "pay", 30.0) == 1000.0
    assert calc.duplicate_alerts(ev, 0.0) == 1 and calc.incidents_opened(ev, 0.0) == 1
    assert calc.rebalance_time_ms(ev, 40.0) == 6000.0
    results = [{"service_id": "pay", "ts": t} for t in (1.0, 2.0, 3.0, 7.0, 8.0)]
    g = calc.max_probe_gap_s(results, ["pay"], 0.0, 9.0)
    assert g["max_gap_s"] == 4.0
    assert calc.coverage_pct([(0.0, 1.0), (1.0, 0.5), (2.0, 1.0)]) == 75.0


# ----------------------------------------------------------------------------- on real nodes
def test_probes_are_spread_over_nodes_with_two_probers_each(tmp_path):
    async def scenario():
        async with running_sentinel(tmp_path) as env:
            v = await env.view()
            assert all(len(s["probers"]) == 2 for s in v["services"])
            assert len([n for n, sv in v["assignments"].items() if sv]) >= 4
            loads = [len(x) for x in v["assignments"].values()]
            assert max(loads) - min(loads) <= 1
            assert v["summary"]["coverage_pct"] == 100.0
    run(scenario())


def test_outage_opens_one_incident_alerts_once_and_resolves(tmp_path):
    async def scenario():
        async with running_sentinel(tmp_path) as env:
            t0 = time.time()
            await env.chaos("payments-api", "down")
            await wait_until(lambda: _open(env, "payments-api"), timeout=10)
            inc = (await _open(env, "payments-api"))
            assert inc["severity"] == "critical" and inc["kind"] == "down" and inc["opened_ts"] >= t0
            await wait_until(lambda: _alert(env, inc["incident_id"], "opened"), timeout=5)
            assert await env.status_of("catalog-api") == UP                 # other services unaffected
            await env.chaos("payments-api", "ok")
            await wait_until(lambda: _state(env, inc["incident_id"], "resolved"), timeout=10)
            await wait_until(lambda: _alert(env, inc["incident_id"], "resolved"), timeout=5)
            incs = [i for i in await env.incidents() if i["service_id"] == "payments-api"]
            assert len(incs) == 1
            keys = [a["dedupe_key"] for a in await env.alerts()]
            assert sorted(keys) == sorted({f"{inc['incident_id']}:opened", f"{inc['incident_id']}:resolved"})
    run(scenario())


def test_slow_service_is_degraded_with_a_warning_incident(tmp_path):
    async def scenario():
        async with running_sentinel(tmp_path) as env:
            await env.chaos("search-api", "slow", latency_ms=300)
            await wait_until(lambda: _status(env, "search-api", DEGRADED), timeout=10)
            inc = await _open(env, "search-api")
            assert inc and inc["kind"] == "degraded" and inc["severity"] == "warning"
            await env.chaos("search-api", "ok")
            await wait_until(lambda: _status(env, "search-api", UP), timeout=10)
    run(scenario())


def test_prober_node_failure_reassigns_checks_without_a_false_outage(tmp_path):
    async def scenario():
        async with running_sentinel(tmp_path) as env:
            v = await env.view()
            victim = max((n for n in v["assignments"] if int(n) != 5), key=lambda n: len(v["assignments"][n]))
            victim = int(victim)
            assert v["assignments"][str(victim)] or v["assignments"][victim]
            await env.cl.crash(victim)

            async def moved():
                vv = await env.view()
                return vv and str(victim) not in vv["assignments"] and victim not in vv["assignments"] and \
                    all(len(s["probers"]) == 2 for s in vv["services"])
            await wait_until(moved, timeout=12)
            await _sleep(1.0)
            vv = await env.view()
            assert all(s["status"] == UP for s in vv["services"]) and vv["summary"]["open_incidents"] == 0
    run(scenario())


def test_open_incident_survives_coordinator_failover_without_duplicate_alert(tmp_path):
    async def scenario():
        async with running_sentinel(tmp_path) as env:
            await env.chaos("payments-api", "down")
            await wait_until(lambda: _open(env, "payments-api"), timeout=10)
            inc = await _open(env, "payments-api")
            await wait_until(lambda: _alert(env, inc["incident_id"], "opened"), timeout=5)
            await env.cl.crash(5)
            await wait_until(lambda: env.cl.agreed_coordinator(expected=4), timeout=15)

            async def continues():
                v = await env.view()
                sv = next((s for s in v["services"] if s["id"] == "payments-api"), None) if v else None
                return sv and sv["status"] == DOWN and sv["open_incident"] == inc["incident_id"]
            await wait_until(continues, timeout=10)
            await env.chaos("payments-api", "ok")
            await wait_until(lambda: _state(env, inc["incident_id"], "resolved"), timeout=12)
            await wait_until(lambda: _alert(env, inc["incident_id"], "resolved"), timeout=5)
            alerts = await env.alerts()                                      # replicated history, includes node 5's alert
            assert sorted(a["dedupe_key"] for a in alerts) == sorted({f"{inc['incident_id']}:opened",
                                                                       f"{inc['incident_id']}:resolved"})
            assert {a["event"]: a["node_id"] for a in alerts} == {"opened": 5, "resolved": 4}   # two coordinators, one story
            assert len([i for i in await env.incidents() if i["service_id"] == "payments-api"]) == 1
            ev = await env.cl.all_events()                                   # live nodes only: nothing delivered twice
            resolved = [e for e in ev["node_events"] if e["event_type"] == "ALERT_DISPATCHED"
                        and e["details"]["event"] == "resolved"]
            assert len(resolved) == 1
    run(scenario())


def test_restarted_coordinator_does_not_resurrect_a_resolved_incident(tmp_path):
    async def scenario():
        async with running_sentinel(tmp_path) as env:
            await env.chaos("auth-api", "down")
            await wait_until(lambda: _open(env, "auth-api"), timeout=10)
            inc = await _open(env, "auth-api")
            await env.cl.crash(5)
            await wait_until(lambda: env.cl.agreed_coordinator(expected=4), timeout=15)
            await env.chaos("auth-api", "ok")
            await wait_until(lambda: _state(env, inc["incident_id"], "resolved"), timeout=12)
            await env.cl.start(5)                                           # node 5 still has the incident OPEN in its journal
            await wait_until(lambda: env.cl.agreed_coordinator(expected=5, min_epoch=3), timeout=15)

            async def settled():
                v = await env.view()
                return v and v["summary"]["open_incidents"] == 0 and all(s["status"] == UP for s in v["services"]) \
                    and all(len(s["probers"]) == 2 for s in v["services"])
            await wait_until(settled, timeout=15)
            incs = await env.incidents()
            assert [i["state"] for i in incs if i["service_id"] == "auth-api"] == ["resolved"]
    run(scenario())


# ----------------------------------------------------------------------------- helpers
async def _sleep(s):
    import asyncio
    await asyncio.sleep(s)


async def _open(env, service_id):
    return next((i for i in await env.incidents() if i["service_id"] == service_id and i["state"] == "open"), None)


async def _state(env, incident_id, state):
    return any(i["incident_id"] == incident_id and i["state"] == state for i in await env.incidents())


async def _alert(env, incident_id, event):
    return any(a["dedupe_key"] == f"{incident_id}:{event}" for a in await env.alerts())


async def _status(env, service_id, status):
    return (await env.status_of(service_id)) == status
