"""Configured node addresses. The registry is static: membership health lives elsewhere."""
from __future__ import annotations

from typing import Dict, List

from node.config import ClusterConfig, NodeSpec


class PeerRegistry:
    def __init__(self, config: ClusterConfig, self_id: int | None = None):
        self._nodes: Dict[int, NodeSpec] = {n.id: n for n in config.nodes}
        self.self_id = self_id

    def get(self, node_id: int) -> NodeSpec:
        return self._nodes[node_id]

    def url(self, node_id: int, path: str = "") -> str:
        return f"{self._nodes[node_id].base_url}{path}"

    def priority(self, node_id: int) -> int:
        return self._nodes[node_id].priority

    def all(self) -> List[NodeSpec]:
        return sorted(self._nodes.values(), key=lambda n: n.id)

    def peers(self) -> List[NodeSpec]:
        return [n for n in self.all() if n.id != self.self_id]

    def higher_priority_than(self, priority: int) -> List[NodeSpec]:
        return [n for n in self.all() if n.priority > priority]

    def __contains__(self, node_id: int) -> bool:
        return node_id in self._nodes
