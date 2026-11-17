"""Starts ONE node process:  python -m node.main --id 3"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

if __package__ in (None, ""):                      # allow `python node/main.py --id 3`
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import uvicorn

from node.config import load_config
from node.service import NodeRuntime, create_app


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Run one monitoring node")
    parser.add_argument("--id", type=int, required=True, help="node id from config/nodes.yaml")
    parser.add_argument("--config", default=None, help="path to nodes.yaml (default: config/nodes.yaml)")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(level=args.log_level.upper(), stream=sys.stdout,
                        format=f"%(asctime)s [node {args.id}] %(message)s", datefmt="%H:%M:%S")
    for noisy in ("httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    config = load_config(args.config)
    spec = config.node(args.id)
    runtime = NodeRuntime(config, args.id)
    app = create_app(runtime)
    uvicorn.run(app, host=spec.host, port=spec.port, log_level="warning", access_log=False)


if __name__ == "__main__":
    main()
