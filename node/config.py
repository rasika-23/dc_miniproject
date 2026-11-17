"""Loads the cluster configuration (node addresses, priorities and timing)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "nodes.yaml"


@dataclass(frozen=True)
class NodeSpec:
    id: int
    priority: int
    host: str
    port: int

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"


@dataclass
class Timing:
    heartbeat_interval: float = 1.0
    suspicion_timeout: float = 3.0
    failure_confirmation: float = 2.0
    coordinator_heartbeat_interval: float = 1.0
    coordinator_timeout: float = 4.0
    election_timeout: float = 1.5
    coordinator_wait_timeout: float = 3.0
    election_max_retries: int = 3
    election_stagger: float = 0.15
    startup_grace: float = 1.5
    request_timeout: float = 1.0
    detector_tick: float = 0.5
    status_sync_interval: float = 2.0
    recovery_validation_beats: int = 2
    heartbeat_log_every: int = 10

    @property
    def failure_timeout(self) -> float:
        """Total silence after which a worker is declared FAILED."""
        return self.suspicion_timeout + self.failure_confirmation


@dataclass
class ClusterConfig:
    nodes: List[NodeSpec]
    timing: Timing = field(default_factory=Timing)
    faults_enabled: bool = True
    data_dir: Path = PROJECT_ROOT / "data"
    runtime_dir: Path = PROJECT_ROOT / "runtime"
    source: Optional[Path] = None

    def __post_init__(self) -> None:
        self.nodes = sorted(self.nodes, key=lambda n: n.id)
        self.data_dir = Path(self.data_dir)
        self.runtime_dir = Path(self.runtime_dir)

    # ----- lookups -----
    def node(self, node_id: int) -> NodeSpec:
        for n in self.nodes:
            if n.id == node_id:
                return n
        raise KeyError(f"Unknown node id {node_id}")

    def peers_of(self, node_id: int) -> List[NodeSpec]:
        return [n for n in self.nodes if n.id != node_id]

    @property
    def node_ids(self) -> List[int]:
        return [n.id for n in self.nodes]

    @property
    def max_priority(self) -> int:
        return max(n.priority for n in self.nodes)

    def node_db_path(self, node_id: int) -> Path:
        return self.data_dir / f"node_{node_id}.db"

    @property
    def controller_db_path(self) -> Path:
        return self.data_dir / "controller.db"


def _resolve(path_value: str, base: Path) -> Path:
    p = Path(path_value)
    return p if p.is_absolute() else (base / p)


def load_config(path: Optional[os.PathLike | str] = None) -> ClusterConfig:
    """Load config from `path`, $DMP_CONFIG, or config/nodes.yaml (in that order)."""
    cfg_path = Path(path or os.environ.get("DMP_CONFIG") or DEFAULT_CONFIG_PATH)
    if not cfg_path.is_absolute():
        cfg_path = (Path.cwd() / cfg_path) if (Path.cwd() / cfg_path).exists() else (PROJECT_ROOT / cfg_path)
    with open(cfg_path, "r", encoding="utf-8") as fh:
        raw: Dict[str, Any] = yaml.safe_load(fh) or {}

    nodes = [NodeSpec(int(n["id"]), int(n["priority"]), str(n.get("host", "127.0.0.1")), int(n["port"]))
             for n in raw.get("nodes", [])]
    if not nodes:
        raise ValueError(f"No nodes defined in {cfg_path}")

    timing_raw = raw.get("timing", {}) or {}
    valid = {f.name for f in fields(Timing)}
    unknown = set(timing_raw) - valid
    if unknown:
        raise ValueError(f"Unknown timing keys in {cfg_path}: {sorted(unknown)}")
    timing = Timing(**timing_raw)

    storage = raw.get("storage", {}) or {}
    return ClusterConfig(
        nodes=nodes,
        timing=timing,
        faults_enabled=bool((raw.get("faults", {}) or {}).get("enabled", True)),
        data_dir=_resolve(storage.get("data_dir", "data"), PROJECT_ROOT),
        runtime_dir=_resolve(storage.get("runtime_dir", "runtime"), PROJECT_ROOT),
        source=cfg_path,
    )
