"""Restarted nodes: new incarnation, re-registration, validation and leadership reclaim."""
import time

from tests.conftest import run, running_cluster, wait_until


async def _healthy(cl):
    m = await cl.membership()
    return len(m) == len(cl.cfg.nodes) and all(x["health"] == "HEALTHY" for x in m.values())


async def _is(cl, node_id, health):
    return (await cl.health_of(node_id)) == health


def test_restarted_worker_goes_recovering_then_healthy(tmp_path):
    async def scenario():
        async with running_cluster(tmp_path) as cl:
            await wait_until(lambda: cl.agreed_coordinator(expected=5), timeout=15)
            await wait_until(lambda: _healthy(cl), timeout=10)
            old_inc = (await cl.get(2, "/health"))["incarnation"]
            await cl.crash(2)
            await wait_until(lambda: _is(cl, 2, "FAILED"), timeout=8)
            t_restart = time.time()
            await cl.start(2)
            await wait_until(lambda: _is(cl, 2, "HEALTHY"), timeout=12)
            new = await cl.get(2, "/health")
            assert new["incarnation"] > old_inc
            ev = await cl.all_events(since=t_restart - 0.2)
            seq = [e["event_type"] for e in ev["node_events"]
                   if e.get("subject_node_id") == 2 and e["event_type"] in ("NODE_RECOVERING", "NODE_HEALTHY")]
            assert seq[:2] == ["NODE_RECOVERING", "NODE_HEALTHY"]
            healthy = next(e for e in ev["node_events"] if e["event_type"] == "NODE_HEALTHY" and e["subject_node_id"] == 2)
            assert healthy["details"]["validated_beats"] >= 2
            # no election was needed for a worker restart
            assert (await cl.get(5, "/health"))["role"] == "coordinator"
    run(scenario())


def test_recovered_coordinator_reclaims_leadership_with_new_epoch(tmp_path):
    async def scenario():
        async with running_cluster(tmp_path) as cl:
            await wait_until(lambda: cl.agreed_coordinator(expected=5), timeout=15)
            await cl.crash(5)
            await wait_until(lambda: cl.agreed_coordinator(expected=4), timeout=15)
            e4 = (await cl.get(4, "/health"))["epoch"]
            await cl.start(5)
            await wait_until(lambda: cl.agreed_coordinator(expected=5, min_epoch=e4 + 1), timeout=15)
            views = await cl.views()
            assert views[4]["role"] == "worker" and views[5]["epoch"] > e4
            await wait_until(lambda: _healthy(cl), timeout=10)
    run(scenario())


def test_restart_recovers_epoch_from_local_journal(tmp_path):
    async def scenario():
        async with running_cluster(tmp_path) as cl:
            await wait_until(lambda: cl.agreed_coordinator(expected=5), timeout=15)
            await cl.crash(5)
            await wait_until(lambda: cl.agreed_coordinator(expected=4), timeout=15)
            highest = (await cl.get(4, "/health"))["epoch"]
            await cl.crash(1)
            rt = await cl.start(1)
            assert rt.coord.highest_epoch_seen >= 1          # read back from node_1.db, not from peers
            await wait_until(lambda: cl.agreed_coordinator(expected=4), timeout=15)
            assert (await cl.get(1, "/health"))["epoch"] == highest
    run(scenario())
