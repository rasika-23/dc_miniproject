"""Suspected / failed transitions, no needless elections, and fault-injected false positives."""
import time

from communication.peer_registry import PeerRegistry
from monitoring.membership_registry import MembershipRegistry
from node.node_state import Health
from tests.conftest import FAST, InProcessCluster, run, running_cluster, wait_until


def test_timeouts_move_worker_through_suspected_to_failed(tmp_path):
    cl = InProcessCluster(tmp_path)
    reg = MembershipRegistry(PeerRegistry(cl.cfg, 5), 5, FAST)
    reg.reset(1000)
    reg.on_register(2, 2000)
    t0 = reg.members[2].last_seen_mono
    only2 = lambda trs: [(t.node_id, t.kind) for t in trs if t.node_id == 2]   # other members are UNKNOWN here
    assert only2(reg.check_timeouts(now=t0 + FAST.suspicion_timeout - 0.05)) == []
    assert only2(reg.check_timeouts(now=t0 + FAST.suspicion_timeout + 0.05)) == [(2, "suspected")]
    assert only2(reg.check_timeouts(now=t0 + FAST.suspicion_timeout + 0.1)) == []   # still only suspected
    assert only2(reg.check_timeouts(now=t0 + FAST.failure_timeout + 0.05)) == [(2, "failed")]
    assert reg.members[2].failure_count == 1


def test_heartbeat_while_suspected_clears_suspicion(tmp_path):
    cl = InProcessCluster(tmp_path)
    reg = MembershipRegistry(PeerRegistry(cl.cfg, 5), 5, FAST)
    reg.reset(1000)
    reg.on_register(2, 2000)
    t0 = reg.members[2].last_seen_mono
    reg.check_timeouts(now=t0 + FAST.suspicion_timeout + 0.05)
    out = reg.on_heartbeat(2, 2000, 1.0, now=t0 + FAST.suspicion_timeout + 0.1)
    assert reg.health_of(2) == Health.HEALTHY
    assert [t.kind for t in out.transitions] == ["suspicion_cleared"]


def test_worker_crash_detected_without_new_election(tmp_path):
    async def scenario():
        async with running_cluster(tmp_path) as cl:
            await wait_until(lambda: cl.agreed_coordinator(expected=5), timeout=15)
            await wait_until(lambda: _all_healthy(cl), timeout=10)
            epoch = (await cl.get(5, "/health"))["epoch"]
            t_fault = await cl.crash(2)
            await wait_until(lambda: _is(cl, 2, "FAILED"), timeout=8)
            await asyncio_sleep(0.6)
            views = await cl.views()
            assert all(v["coordinator_id"] == 5 and v["epoch"] == epoch for v in views.values())
            ev = await cl.all_events(since=t_fault)
            assert not [e for e in ev["election_events"] if e["event_type"] == "ELECTION_STARTED"]
            failed = [e for e in ev["node_events"] if e["event_type"] == "NODE_FAILED" and e["subject_node_id"] == 2]
            suspected = [e for e in ev["node_events"] if e["event_type"] == "NODE_SUSPECTED" and e["subject_node_id"] == 2]
            assert failed and suspected and suspected[0]["ts"] < failed[0]["ts"]
            latency = failed[0]["ts"] - t_fault
            assert FAST.suspicion_timeout - 0.3 < latency < FAST.failure_timeout + 1.5, latency
    run(scenario())


def test_long_heartbeat_delay_causes_false_failure_that_heals(tmp_path):
    async def scenario():
        async with running_cluster(tmp_path) as cl:
            await wait_until(lambda: cl.agreed_coordinator(expected=5), timeout=15)
            await wait_until(lambda: _all_healthy(cl), timeout=10)
            t0 = time.time()
            r = await cl.post(3, "/faults", {"sender_id": 0, "incarnation": 0, "action": "apply",
                                              "fault_type": "heartbeat_delay", "delay_ms": 2500, "duration_s": 4.0})
            assert r.status_code == 200
            await wait_until(lambda: _is(cl, 3, "FAILED"), timeout=8)          # false positive: node 3 is alive
            await wait_until(lambda: _is(cl, 3, "HEALTHY"), timeout=12)         # ... and it heals itself
            ev = await cl.all_events(since=t0)
            kinds = [e["event_type"] for e in ev["node_events"] if e.get("subject_node_id") == 3]
            for needed in ("NODE_SUSPECTED", "NODE_FAILED", "NODE_RECOVERING", "NODE_HEALTHY"):
                assert needed in kinds
    run(scenario())


def test_total_heartbeat_loss_then_clear(tmp_path):
    async def scenario():
        async with running_cluster(tmp_path) as cl:
            await wait_until(lambda: cl.agreed_coordinator(expected=5), timeout=15)
            await wait_until(lambda: _all_healthy(cl), timeout=10)
            await cl.post(1, "/faults", {"sender_id": 0, "incarnation": 0, "action": "apply",
                                          "fault_type": "message_loss", "loss_pct": 100, "scope": "heartbeat"})
            await wait_until(lambda: _is(cl, 1, "FAILED"), timeout=8)
            counters = (await cl.get(1, "/cluster/metrics"))["counters"]
            assert counters["dropped"] > 0
            await cl.post(1, "/faults", {"sender_id": 0, "incarnation": 0, "action": "clear"})
            await wait_until(lambda: _is(cl, 1, "HEALTHY"), timeout=12)
    run(scenario())


def test_fault_endpoint_validates_input(tmp_path):
    async def scenario():
        async with running_cluster(tmp_path, ids=[1]) as cl:
            bad = await cl.post(1, "/faults", {"sender_id": 0, "incarnation": 0, "action": "apply",
                                                "fault_type": "message_loss", "loss_pct": 0})
            assert bad.status_code == 422
            unknown = await cl.post(1, "/faults", {"sender_id": 0, "incarnation": 0, "action": "apply",
                                                    "fault_type": "explode"})
            assert unknown.status_code == 422
    run(scenario())


# ----------------------------------------------------------------------------- helpers
async def asyncio_sleep(s):
    import asyncio
    await asyncio.sleep(s)


async def _is(cl, node_id, health):
    return (await cl.health_of(node_id)) == health


async def _all_healthy(cl):
    m = await cl.membership()
    return len(m) == len(cl.cfg.nodes) and all(x["health"] == "HEALTHY" for x in m.values())
