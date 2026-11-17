"""Coordinator crash -> timeout -> Bully election -> next-highest node leads."""
from tests.conftest import FAST, run, running_cluster, wait_until


async def _healthy(cl):
    m = await cl.membership()
    return len(m) == len(cl.cfg.nodes) and all(x["health"] == "HEALTHY" for x in m.values())


def test_node_4_takes_over_when_node_5_crashes(tmp_path):
    async def scenario():
        async with running_cluster(tmp_path) as cl:
            await wait_until(lambda: cl.agreed_coordinator(expected=5), timeout=15)
            await wait_until(lambda: _healthy(cl), timeout=10)
            epoch = (await cl.get(5, "/health"))["epoch"]
            t_fault = await cl.crash(5)
            await wait_until(lambda: cl.agreed_coordinator(expected=4, min_epoch=epoch + 1), timeout=15)
            views = await cl.views()
            assert views[4]["role"] == "coordinator" and views[4]["epoch"] == epoch + 1
            assert all(v["epoch"] == epoch + 1 for v in views.values())
            ev = await cl.all_events(since=t_fault)
            timeouts = [e for e in ev["node_events"] if e["event_type"] == "COORDINATOR_TIMEOUT"]
            assert timeouts and all(e["subject_node_id"] == 5 for e in timeouts)
            ann = [e for e in ev["election_events"] if e["event_type"] == "COORDINATOR_ANNOUNCED"]
            assert ann and ann[0]["node_id"] == 4
            failover = ann[0]["ts"] - t_fault
            assert failover < FAST.coordinator_timeout + FAST.election_timeout + 1.5, failover
            # the new coordinator rebuilds membership and declares the dead node failed
            await wait_until(lambda: _state(cl, 5, "FAILED"), timeout=8)
    run(scenario())


def test_consecutive_failovers_walk_down_the_priorities(tmp_path):
    async def scenario():
        async with running_cluster(tmp_path) as cl:
            await wait_until(lambda: cl.agreed_coordinator(expected=5), timeout=15)
            await cl.crash(5)
            await wait_until(lambda: cl.agreed_coordinator(expected=4), timeout=15)
            e4 = (await cl.get(4, "/health"))["epoch"]
            await cl.crash(4)
            await wait_until(lambda: cl.agreed_coordinator(expected=3, min_epoch=e4 + 1), timeout=15)
            views = await cl.views()
            assert sorted(views) == [1, 2, 3] and views[3]["role"] == "coordinator"
    run(scenario())


def test_worker_cluster_survives_without_dashboard_or_discovery_client(tmp_path):
    """Nothing but the nodes themselves is needed for monitoring and election to continue."""
    async def scenario():
        async with running_cluster(tmp_path) as cl:
            await wait_until(lambda: cl.agreed_coordinator(expected=5), timeout=15)
            await cl.crash(5)                     # and no client polls anything meanwhile
            import asyncio
            await asyncio.sleep(FAST.coordinator_timeout + FAST.election_timeout + 1.0)
            assert await cl.agreed_coordinator(expected=4) == 4
    run(scenario())


async def _state(cl, node_id, health):
    m = await cl.membership()
    return node_id in m and m[node_id]["health"] == health
