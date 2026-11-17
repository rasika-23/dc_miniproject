"""Start all node processes (default: 5) as detached background processes.

    python scripts/start_cluster.py            # start and wait for the first coordinator
    python scripts/start_cluster.py --fresh    # wipe node databases first
    python scripts/start_cluster.py --nodes 1,2,3
"""
import _bootstrap  # noqa: F401  (adds project root to sys.path)

import argparse
import shutil
import time

from election.leader_discovery import STATE_OPERATIONAL
from fault_injection.controller import ClusterController
from node.config import load_config


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=None)
    ap.add_argument("--nodes", default=None, help="comma-separated node ids (default: all)")
    ap.add_argument("--fresh", action="store_true", help="delete existing node databases and logs first")
    ap.add_argument("--no-wait", action="store_true", help="do not wait for an elected coordinator")
    args = ap.parse_args()

    cfg = load_config(args.config)
    ids = [int(x) for x in args.nodes.split(",")] if args.nodes else cfg.node_ids
    ctrl = ClusterController(cfg)
    if args.fresh:
        from fault_injection import process_control
        process_control.stop_all(cfg)
        time.sleep(0.5)
        for p in cfg.data_dir.glob("*.db*"):
            p.unlink(missing_ok=True)
        shutil.rmtree(cfg.runtime_dir / "logs", ignore_errors=True)
        ctrl = ClusterController(cfg)
    ctrl.start_cluster(ids)
    print(f"Started nodes {ids}. Logs: {cfg.runtime_dir / 'logs'}")
    if args.no_wait:
        return
    print("Waiting for the first election ...")
    d = ctrl.wait_for_coordinator(timeout=60)
    if d and d.state == STATE_OPERATIONAL:
        print(f"Coordinator: node {d.coordinator_id} (epoch {d.epoch}); reachable nodes: {d.reachable}")
        print("Open the dashboard with:  streamlit run dashboard/app.py")
    else:
        print("No coordinator elected yet - check the node logs.")


if __name__ == "__main__":
    main()
