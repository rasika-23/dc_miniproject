"""External process control: start, crash and restart node processes.

This is deliberately separate from the node API, so the coordinator can be killed even though
it is the very process under test. Works on Windows, macOS and Linux (via psutil).
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

import psutil

from node.config import PROJECT_ROOT, ClusterConfig, load_config


def _pids_file(cfg: ClusterConfig) -> Path:
    cfg.runtime_dir.mkdir(parents=True, exist_ok=True)
    return cfg.runtime_dir / "pids.json"


def _read_pids(cfg: ClusterConfig) -> Dict[str, dict]:
    f = _pids_file(cfg)
    try:
        return json.loads(f.read_text(encoding="utf-8")) if f.exists() else {}
    except (OSError, ValueError):
        return {}


def _write_pids(cfg: ClusterConfig, data: Dict[str, dict]) -> None:
    _pids_file(cfg).write_text(json.dumps(data, indent=2), encoding="utf-8")


def _is_node_cmdline(cmdline: List[str], node_id: int) -> bool:
    if not cmdline:
        return False
    joined = " ".join(cmdline)
    return "node.main" in joined.replace("\\", "/").replace("node/main.py", "node.main") and \
        any(a == "--id" and i + 1 < len(cmdline) and cmdline[i + 1] == str(node_id) for i, a in enumerate(cmdline))


def find_node_pids(node_id: int, cfg: Optional[ClusterConfig] = None) -> List[int]:
    """All PIDs belonging to a node: the recorded one, plus any process with a matching command line."""
    pids = set()
    if cfg is not None:
        rec = _read_pids(cfg).get(str(node_id))
        if rec and psutil.pid_exists(rec["pid"]):
            pids.add(rec["pid"])
    for proc in psutil.process_iter(["pid", "cmdline"]):
        try:
            if _is_node_cmdline(proc.info.get("cmdline") or [], node_id):
                pids.add(proc.info["pid"])
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return sorted(pids)


def recorded_status(node_id: int, cfg: Optional[ClusterConfig] = None) -> Dict[str, object]:
    """Cheap status from pids.json only (no process scan) - safe to call on every dashboard refresh."""
    cfg = cfg or load_config()
    rec = _read_pids(cfg).get(str(node_id))
    if not rec:
        return {"pid": None, "running": False, "started_at": None}
    running = False
    try:
        p = psutil.Process(rec["pid"])
        running = p.is_running() and p.status() != psutil.STATUS_ZOMBIE
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        running = False
    return {"pid": rec["pid"], "running": running, "started_at": rec.get("started_at")}


def start_node(node_id: int, cfg: Optional[ClusterConfig] = None, config_path: Optional[str] = None) -> int:
    """Launch `python -m node.main --id N` detached from this process. Returns the PID."""
    cfg = cfg or load_config(config_path)
    cfg.node(node_id)  # validates the id
    existing = [p for p in find_node_pids(node_id, cfg)]
    if existing:
        return existing[0]
    log_dir = cfg.runtime_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    cfg.data_dir.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, "-m", "node.main", "--id", str(node_id)]
    if config_path or (cfg.source and Path(cfg.source) != PROJECT_ROOT / "config" / "nodes.yaml"):
        cmd += ["--config", str(config_path or cfg.source)]
    log_file = open(log_dir / f"node{node_id}.log", "ab")
    kwargs: dict = {"cwd": str(PROJECT_ROOT), "stdout": log_file, "stderr": subprocess.STDOUT,
                    "stdin": subprocess.DEVNULL}
    if os.name == "nt":
        kwargs["creationflags"] = (subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
                                   | getattr(subprocess, "CREATE_NO_WINDOW", 0))
    else:
        kwargs["start_new_session"] = True
    env = dict(os.environ, PYTHONUNBUFFERED="1", PYTHONPATH=str(PROJECT_ROOT))
    proc = subprocess.Popen(cmd, env=env, **kwargs)
    log_file.close()
    pids = _read_pids(cfg)
    pids[str(node_id)] = {"pid": proc.pid, "started_at": time.time()}
    _write_pids(cfg, pids)
    return proc.pid


def stop_node(node_id: int, cfg: Optional[ClusterConfig] = None, timeout: float = 5.0) -> bool:
    """Hard-kill a node (simulates a crash: no goodbye message). Returns True if something was killed."""
    cfg = cfg or load_config()
    victims: List[psutil.Process] = []
    for pid in find_node_pids(node_id, cfg):
        try:
            p = psutil.Process(pid)
            victims.extend(p.children(recursive=True))
            victims.append(p)
        except psutil.NoSuchProcess:
            continue
    for p in victims:
        try:
            p.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    if victims:
        psutil.wait_procs(victims, timeout=timeout)
    pids = _read_pids(cfg)
    if pids.pop(str(node_id), None) is not None:
        _write_pids(cfg, pids)
    return bool(victims)


def restart_node(node_id: int, cfg: Optional[ClusterConfig] = None) -> int:
    """Start a node that was stopped. A new process means a new incarnation."""
    return start_node(node_id, cfg)


def stop_all(cfg: Optional[ClusterConfig] = None) -> List[int]:
    cfg = cfg or load_config()
    return [n.id for n in cfg.nodes if stop_node(n.id, cfg)]
