"""Stable person ID reconciliation (DESIGN.md section 11, D-005).

Matching recomputes every cluster each load (D-004). This module then carries
person IDs over from the previous load, so that only records whose cluster
changed get a different ID.

Records are identified by their *record uid*, ``SOURCE|ID|LIFE``: a source ID
reused for a new record (SOURCE_ID_REUSE) is a new record and does not
inherit the old record's person ID (D-041).

Rules:

1. Each previous ID *claims* the new cluster holding most of its surviving
   records (ties: the cluster with the smallest cluster id).
2. A cluster claimed by one or more IDs keeps the oldest (lowest sequence
   number). The others are retired as ``MERGED``, pointing to the survivor.
3. A cluster claimed by no ID gets a new ID, including clusters that hold
   only the minority of an ID that was split.
4. An ID none of whose records survive is retired as ``ALL_DELETED``.
5. New IDs come from a monotonic sequence, in cluster id order. Retired IDs
   are never reissued, because no rule reads them again.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass

from pipeline.config import PersonIdFormat

MERGED, ALL_DELETED = "MERGED", "ALL_DELETED"


@dataclass(frozen=True)
class Retirement:
    retired_id: str
    successor_id: str | None
    reason: str


@dataclass(frozen=True)
class Reconciliation:
    person_of: dict[str, str]  # record uid -> person id
    retirements: tuple[Retirement, ...]
    next_sequence: int
    new_ids: tuple[str, ...]


def id_sequence(person_id: str, id_format: PersonIdFormat) -> int:
    """The sequence number inside a person ID (its age: lower is older)."""
    if not person_id.startswith(id_format.prefix):
        raise ValueError(f"{person_id!r} does not start with {id_format.prefix!r}")
    return int(person_id[len(id_format.prefix) :])


def reconcile(
    cluster_of: dict[str, str],
    previous_person_of: dict[str, str],
    next_sequence: int,
    id_format: PersonIdFormat,
) -> Reconciliation:
    """Assign person IDs to this load's clusters.

    ``cluster_of`` maps each live record uid to its cluster id;
    ``previous_person_of`` is the previous load's assignment (empty at load 0);
    ``next_sequence`` is the first unused sequence number.
    """
    counts: dict[str, Counter] = defaultdict(Counter)
    for uid, person_id in previous_person_of.items():
        cluster = cluster_of.get(uid)
        if cluster is not None:
            counts[person_id][cluster] += 1

    def age(person_id: str) -> tuple[int, str]:
        return (id_sequence(person_id, id_format), person_id)

    claims: dict[str, list[str]] = defaultdict(list)
    for person_id, per_cluster in counts.items():
        home = min(per_cluster, key=lambda cluster: (-per_cluster[cluster], cluster))
        claims[home].append(person_id)

    retirements = [
        Retirement(person_id, None, ALL_DELETED)
        for person_id in sorted(set(previous_person_of.values()) - counts.keys(), key=age)
    ]
    person_of_cluster: dict[str, str] = {}
    new_ids = []
    for cluster in sorted(set(cluster_of.values())):
        claimants = sorted(claims.get(cluster, ()), key=age)
        if claimants:
            survivor = claimants[0]
            retirements.extend(Retirement(p, survivor, MERGED) for p in claimants[1:])
        else:
            survivor = id_format.format(next_sequence)
            next_sequence += 1
            new_ids.append(survivor)
        person_of_cluster[cluster] = survivor

    return Reconciliation(
        person_of={uid: person_of_cluster[cluster] for uid, cluster in sorted(cluster_of.items())},
        retirements=tuple(sorted(retirements, key=lambda r: age(r.retired_id))),
        next_sequence=next_sequence,
        new_ids=tuple(new_ids),
    )
