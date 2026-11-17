"""Run every demo service (those with a control_url in config) inside one process:
    python -m demo_services.main
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from urllib.parse import urlparse

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import uvicorn

from demo_services.app import make_app
from node.config import load_config


async def serve_all(config_path: str | None) -> None:
    cfg = load_config(config_path)
    servers = []
    for spec in cfg.services:
        if not spec.control_url:
            continue
        u = urlparse(spec.url)
        server = uvicorn.Server(uvicorn.Config(make_app(spec.id, spec.name), host=u.hostname, port=u.port,
                                               log_level="warning", access_log=False))
        servers.append(server.serve())
        logging.info("demo service %s on %s", spec.id, spec.url)
    if not servers:
        logging.warning("no demo services configured (services with control_url)")
        return
    await asyncio.gather(*servers)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=None)
    args = ap.parse_args()
    logging.basicConfig(level="INFO", stream=sys.stdout, format="%(asctime)s [demo services] %(message)s",
                        datefmt="%H:%M:%S")
    asyncio.run(serve_all(args.config))


if __name__ == "__main__":
    main()
