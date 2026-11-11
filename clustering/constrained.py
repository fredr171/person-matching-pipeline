"""Constrained, deterministic clustering (DESIGN.md section 10, D-006).

Pairwise matches are not transitive. Clustering joins records greedily,
strongest evidence first, and refuses any join that would put a cannot-link
pair (a blocked pair) into one cluster:

1. Connected components over the match edges bound the work: records in
   different components can never share a cluster.
2. Within a component, match edges are taken in order of tier, then
   ``(min record key, max record key)``.
3. Union-find joins the two clusters of an edge unless a cannot-link pair
   spans them. Refused joins are recorded as ``SKIPPED_TRANSITIVITY``, with
   the first conflicting cannot-link pair as evidence.

A cluster is identified by its smallest record key, so cluster IDs do not
depend on processing order. Both engines call :func:`cluster_component` per
component; dbt calls :func:`cluster_records` for everything at once, which
gives the same result.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass

SKIPPED_TRANSITIVITY = "SKIPPED_TRANSITIVITY"


@dataclass(frozen=True)
class MatchEdge:
    record_key_a: str
    record_key_b: str
    tier: int

    @property
    def order(self) -> tuple[int, str, str]:
        low, high = sorted((self.record_key_a, self.record_key_b))
        return (self.tier, low, high)


@dataclass(frozen=True)
class CannotLink:
    record_key_a: str
    record_key_b: str
    reason_code: str


@dataclass(frozen=True)
class SkippedJoin:
    record_key_a: str
    record_key_b: str
    tier: int
    conflict_record_key_a: str
    conflict_record_key_b: str
    conflict_reason_code: str
    reason_code: str = SKIPPED_TRANSITIVITY


@dataclass(frozen=True)
class ClusteringResult:
    cluster_of: dict[str, str]  # record key -> cluster id (its smallest record key)
    skipped: tuple[SkippedJoin, ...]


class _UnionFind:
    def __init__(self, nodes: Iterable[str]):
        self.parent = {n: n for n in nodes}

    def find(self, node: str) -> str:
        root = node
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[node] != root:  # path compression
            self.parent[node], node = root, self.parent[node]
        return root

    def union(self, a: str, b: str) -> str:
        """Join the sets of ``a`` and ``b``; the smaller root key becomes the root."""
        root_a, root_b = self.find(a), self.find(b)
        root, other = sorted((root_a, root_b))
        self.parent[other] = root
        return root


def connected_components(record_keys: Iterable[str], edges: Iterable[MatchEdge]) -> dict[str, str]:
    """Record key -> component id (the component's smallest record key)."""
    keys = set(record_keys)
    edges = list(edges)
    for edge in edges:
        keys.update((edge.record_key_a, edge.record_key_b))
    sets = _UnionFind(sorted(keys))
    for edge in edges:
        sets.union(edge.record_key_a, edge.record_key_b)
    return {key: sets.find(key) for key in sorted(keys)}


def cluster_component(
    record_keys: Iterable[str], edges: Iterable[MatchEdge], cannot_links: Iterable[CannotLink]
) -> ClusteringResult:
    """Greedy constrained union-find over one component (or any set of records)."""
    keys = set(record_keys)
    edges = sorted(edges, key=lambda e: e.order)
    for edge in edges:
        keys.update((edge.record_key_a, edge.record_key_b))

    forbidden: dict[str, dict[str, str]] = defaultdict(dict)  # record -> partner -> reason
    for link in cannot_links:
        if link.record_key_a in keys and link.record_key_b in keys:
            forbidden[link.record_key_a][link.record_key_b] = link.reason_code
            forbidden[link.record_key_b][link.record_key_a] = link.reason_code

    sets = _UnionFind(sorted(keys))
    members = {key: {key} for key in keys}  # root -> members
    skipped = []
    for edge in edges:
        root_a, root_b = sets.find(edge.record_key_a), sets.find(edge.record_key_b)
        if root_a == root_b:
            continue
        conflict = _first_conflict(members[root_a], members[root_b], forbidden)
        if conflict:
            skipped.append(SkippedJoin(edge.record_key_a, edge.record_key_b, edge.tier, *conflict))
            continue
        root = sets.union(root_a, root_b)
        other = root_b if root == root_a else root_a
        members[root] |= members.pop(other)

    return ClusteringResult(
        cluster_of={key: sets.find(key) for key in sorted(keys)}, skipped=tuple(skipped)
    )


def _first_conflict(cluster_a: set[str], cluster_b: set[str], forbidden) -> tuple | None:
    """The smallest cannot-link pair spanning the two clusters, or None."""
    small, large = sorted((cluster_a, cluster_b), key=len)
    conflicts = [
        (*sorted((member, partner)), reason)
        for member in small
        for partner, reason in forbidden.get(member, {}).items()
        if partner in large
    ]
    return min(conflicts) if conflicts else None


def cluster_records(
    record_keys: Iterable[str], edges: Iterable[MatchEdge], cannot_links: Iterable[CannotLink]
) -> ClusteringResult:
    """Cluster all records: split into components, then cluster each one."""
    keys = sorted(set(record_keys))
    edges = list(edges)
    component_of = connected_components(keys, edges)
    by_component_keys: dict[str, list[str]] = defaultdict(list)
    for key, component in component_of.items():
        by_component_keys[component].append(key)
    by_component_edges: dict[str, list[MatchEdge]] = defaultdict(list)
    for edge in edges:
        by_component_edges[component_of[edge.record_key_a]].append(edge)
    by_component_links: dict[str, list[CannotLink]] = defaultdict(list)
    for link in cannot_links:
        component = component_of.get(link.record_key_a)
        if component is not None and component == component_of.get(link.record_key_b):
            by_component_links[component].append(link)

    cluster_of: dict[str, str] = {}
    skipped: list[SkippedJoin] = []
    for component in sorted(by_component_keys):
        result = cluster_component(
            by_component_keys[component],
            by_component_edges[component],
            by_component_links[component],
        )
        cluster_of.update(result.cluster_of)
        skipped.extend(result.skipped)
    return ClusteringResult(
        cluster_of=dict(sorted(cluster_of.items())),
        skipped=tuple(sorted(skipped, key=lambda s: (s.tier, s.record_key_a, s.record_key_b))),
    )
