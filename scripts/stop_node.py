"""Hard-stop one node (simulated crash) or the whole cluster.

    python scripts/stop_node.py 5        # crash node 5 (ground truth is recorded)
    python scripts/stop_node.py --all    # shut everything down
"""
import _bootstrap  # noqa: F401

import argparse

from fault_injection import process_control
from fault_injection.controller import ClusterController
from node.config import load_config


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("node_id", nargs="?", type=int)
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--config", default=None)
    args = ap.parse_args()
    cfg = load_config(args.config)
    if args.all:
        print("Stopped nodes:", process_control.stop_all(cfg) or "none were running")
    elif args.node_id:
        ctrl = ClusterController(cfg)
        ts = ctrl.crash(args.node_id, scenario="manual-cli")
        print(f"Node {args.node_id} killed at {ts:.3f} (unix time)")
    else:
        ap.error("give a node id or --all")


if __name__ == "__main__":
    main()
