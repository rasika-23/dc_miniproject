"""Heartbeat flow, stale message rejection and membership transitions."""
import time

from communication.messages import (CoordinatorHeartbeatMessage, HeartbeatMessage)
from communication.peer_registry import PeerRegistry
from monitoring.membership_registry import MembershipRegistry
from node.node_state import Health
from tests.conftest import FAST, InProcessCluster, run, running_cluster, wait_until


def test_workers_report_to_coordinator_and_receive_coordinator_heartbeats(tmp_path):
    async def scenario():
        async with running_cluster(tmp_path) as cl:
            await wait_until(lambda: cl.agreed_coordinator(expected=5), timeout=15)

            async def all_healthy():
                m = await cl.membership()
                return len(m) == 5 and all(x["health"] == "HEALTHY" for x in m.values()) and \
                    all(x["response_time_ms"] is not None for i, x in m.items() if i != 5)
            await wait_until(all_healthy, timeout=10)
            m = await cl.membership()
            assert all(x["last_heartbeat_age_s"] < 1.0 for x in m.values())
            for i in (1, 2, 3, 4):
                h = await cl.get(i, "/health")
                assert h["registered"] and h["coordinator_silence_s"] < FAST.coordinator_timeout

            async def enough_heartbeats():
                m = await cl.get(1, "/cluster/metrics")
                return m["counters"]["sent"].get("HEARTBEAT", 0) > 3
            await wait_until(enough_heartbeats, timeout=10)
    run(scenario())


def test_stale_incarnation_and_non_coordinator_are_rejected(tmp_path):
    async def scenario():
        async with running_cluster(tmp_path) as cl:
            await wait_until(lambda: cl.agreed_coordinator(expected=5), timeout=15)
            await wait_until(lambda: cl.health_of(2), timeout=5)
            current = (await cl.get(2, "/health"))["incarnation"]
            stale = HeartbeatMessage(sender_id=2, incarnation=current - 10_000, epoch=1).model_dump(mode="json")
            r = (await cl.post(5, "/heartbeat", stale)).json()
            assert r["accepted"] is False and r["reason"] == "stale_incarnation"
            # a worker is not a coordinator and says so
            ok = HeartbeatMessage(sender_id=2, incarnation=current, epoch=1).model_dump(mode="json")
            r = (await cl.post(3, "/heartbeat", ok)).json()
            assert r["accepted"] is False and r["reason"] == "not_coordinator" and r["coordinator_id"] == 5
    run(scenario())


def test_worker_rejects_stale_epoch_coordinator_heartbeat(tmp_path):
    async def scenario():
        async with running_cluster(tmp_path) as cl:
            await wait_until(lambda: cl.agreed_coordinator(expected=5), timeout=15)
            epoch = (await cl.get(1, "/health"))["epoch"]
            msg = CoordinatorHeartbeatMessage(sender_id=4, incarnation=1, epoch=epoch - 1, coordinator_id=4)
            r = (await cl.post(1, "/coordinator-heartbeat", msg.model_dump(mode="json"))).json()
            assert r["accepted"] is False and r["reason"] == "stale_epoch"
            assert (await cl.get(1, "/health"))["coordinator_id"] == 5
    run(scenario())


# ----------------------------------------------------------------------------- membership unit tests
def _registry(cfg):
    reg = MembershipRegistry(PeerRegistry(cfg, 5), 5, FAST)
    reg.reset(self_incarnation=1000)
    return reg


def test_register_then_heartbeat_marks_healthy(tmp_path):
    cl = InProcessCluster(tmp_path)
    reg = _registry(cl.cfg)
    assert reg.health_of(2) == Health.UNKNOWN
    out = reg.on_register(2, incarnation=2000)
    assert out.accepted and reg.health_of(2) == Health.HEALTHY
    out = reg.on_heartbeat(2, 2000, latency_ms=3.0)
    assert out.accepted and not out.need_register and reg.members[2].response_time_ms == 3.0


def test_unknown_incarnation_must_register_first(tmp_path):
    cl = InProcessCluster(tmp_path)
    reg = _registry(cl.cfg)
    out = reg.on_heartbeat(3, 3000, 1.0)
    assert out.accepted and out.need_register
