"""Bully election: decision rules (unit) and election outcomes on real node services."""
from election.bully import claim_key, evaluate_announcement, higher_priority_peers, next_epoch
from node.config import NodeSpec
from tests.conftest import run, running_cluster, wait_until


# ----------------------------------------------------------------------------- pure rules
def _eval(**kw):
    base = dict(my_id=2, my_priority=2, cur_coordinator_id=None, cur_coordinator_priority=None, cur_epoch=0,
                ann_coordinator_id=5, ann_priority=5, ann_epoch=1)
    base.update(kw)
    return evaluate_announcement(**base)


def test_higher_priority_peers_only():
    peers = [NodeSpec(i, i, "h", 8000 + i) for i in (1, 3, 4, 5)]
    assert [p.id for p in higher_priority_peers(2, peers)] == [3, 4, 5]
    assert higher_priority_peers(5, peers) == []


def test_epoch_and_claim_ordering():
    assert next_epoch(3) == 4
    assert claim_key(2, 1) > claim_key(1, 5)          # newer epoch beats higher priority
    assert claim_key(2, 5) > claim_key(2, 4)          # same epoch: higher priority wins


def test_announcement_rules():
    assert _eval().accept                                                   # no coordinator yet
    assert not _eval(cur_epoch=3, ann_epoch=2).accept                       # stale epoch
    assert _eval(cur_coordinator_id=4, cur_coordinator_priority=4, cur_epoch=2, ann_epoch=3).accept
    assert not _eval(ann_coordinator_id=1, ann_priority=1).accept           # never follow a lower priority node
    assert not _eval(ann_coordinator_id=2).accept                           # own announcement
    same = _eval(cur_coordinator_id=5, cur_coordinator_priority=5, cur_epoch=1, ann_epoch=1)
    assert same.accept and same.reason == "same_coordinator"
    tie_lower = _eval(my_id=1, my_priority=1, cur_coordinator_id=4, cur_coordinator_priority=4, cur_epoch=2,
                      ann_coordinator_id=3, ann_priority=3, ann_epoch=2)
    assert not tie_lower.accept and tie_lower.reason == "equal_epoch_lower_priority"
    tie_higher = _eval(my_id=1, my_priority=1, cur_coordinator_id=3, cur_coordinator_priority=3, cur_epoch=2,
                       ann_coordinator_id=4, ann_priority=4, ann_epoch=2)
    assert tie_higher.accept


# ----------------------------------------------------------------------------- on real nodes
def test_initial_election_highest_priority_wins(tmp_path):
    async def scenario():
        async with running_cluster(tmp_path) as cl:
            cid = await wait_until(lambda: cl.agreed_coordinator(expected=5), timeout=15)
            assert cid == 5
            views = await cl.views()
            assert views[5]["role"] == "coordinator" and views[5]["epoch"] >= 1
            assert all(v["role"] == "worker" for i, v in views.items() if i != 5)
            assert len({v["epoch"] for v in views.values()}) == 1
    run(scenario())


def test_election_is_not_hardcoded_to_node_5(tmp_path):
    async def scenario():
        async with running_cluster(tmp_path, ids=[1, 2, 3]) as cl:
            assert await wait_until(lambda: cl.agreed_coordinator(expected=3), timeout=15) == 3
    run(scenario())


def test_higher_priority_latecomer_takes_over(tmp_path):
    async def scenario():
        async with running_cluster(tmp_path, ids=[1, 2, 3]) as cl:
            await wait_until(lambda: cl.agreed_coordinator(expected=3), timeout=15)
            epoch_before = (await cl.views())[3]["epoch"]
            await cl.start(5)
            await wait_until(lambda: cl.agreed_coordinator(expected=5, min_epoch=epoch_before + 1), timeout=15)
            views = await cl.views()
            assert views[3]["role"] == "worker"
    run(scenario())


def test_election_events_are_persisted(tmp_path):
    async def scenario():
        async with running_cluster(tmp_path) as cl:
            await wait_until(lambda: cl.agreed_coordinator(expected=5), timeout=15)
            ev = await cl.events(5)
            types = {e["event_type"] for e in ev["election_events"]}
            assert {"ELECTION_STARTED", "ELECTION_WON", "COORDINATOR_ANNOUNCED"} <= types
            won = next(e for e in ev["election_events"] if e["event_type"] == "ELECTION_WON")
            assert won["epoch"] >= 1 and won["duration_ms"] is not None and won["election_id"]
    run(scenario())
