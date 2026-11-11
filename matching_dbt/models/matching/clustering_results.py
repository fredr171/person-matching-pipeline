"""Constrained clustering through the shared Python function (DESIGN.md section 10, D-006).

Returns one table with two kinds of rows, split by the ``clusters`` and
``skipped_joins`` models:

* ``row_type = 'cluster'``: a record key and its cluster id.
* ``row_type = 'skipped'``: a refused join (``SKIPPED_TRANSITIVITY``).
"""

import pandas as pd

from clustering.constrained import CannotLink, MatchEdge, cluster_records


def model(dbt, session):
    dbt.config(materialized="table")
    records = dbt.ref("int_records_standardized").project("record_key").fetchall()
    pairs = (
        dbt.ref("match_pairs")
        .project("record_key_a, record_key_b, tier, is_match, cannot_link, reason_code")
        .fetchall()
    )

    edges = [MatchEdge(a, b, tier) for a, b, tier, is_match, _, _ in pairs if is_match]
    links = [CannotLink(a, b, reason) for a, b, _, _, cannot, reason in pairs if cannot]
    result = cluster_records([key for (key,) in records], edges, links)

    rows = [
        {"row_type": "cluster", "record_key": key, "cluster_id": cluster}
        for key, cluster in result.cluster_of.items()
    ]
    rows += [
        {
            "row_type": "skipped",
            "record_key_a": s.record_key_a,
            "record_key_b": s.record_key_b,
            "tier": s.tier,
            "conflict_record_key_a": s.conflict_record_key_a,
            "conflict_record_key_b": s.conflict_record_key_b,
            "conflict_reason_code": s.conflict_reason_code,
            "reason_code": s.reason_code,
        }
        for s in result.skipped
    ]
    columns = [
        "row_type",
        "record_key",
        "cluster_id",
        "record_key_a",
        "record_key_b",
        "tier",
        "conflict_record_key_a",
        "conflict_record_key_b",
        "conflict_reason_code",
        "reason_code",
    ]
    frame = pd.DataFrame(rows, columns=columns)
    frame["tier"] = frame["tier"].astype("Int64")
    return frame.astype({c: "string" for c in columns if c != "tier"})
