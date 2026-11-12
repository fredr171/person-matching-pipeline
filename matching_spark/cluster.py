"""Constrained clustering in Spark through the shared Python function (section 10, D-006).

1. Connected components over match edges, by min-label propagation: every
   record repeatedly takes the smallest label among itself and its neighbors
   until nothing changes. The label is then the component's smallest record
   key, the same id ``clustering.constrained.connected_components`` gives.
2. ``groupBy(component).applyInArrow``: each component's members, match edges
   and cannot-links go to ``clustering.constrained.cluster_component``.
   Cannot-links across components can never be violated, so they are dropped.

Records in no match edge are singletons and skip step 2.
"""

from __future__ import annotations

import pyarrow as pa
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from clustering.constrained import CannotLink, MatchEdge, cluster_component

RESULT_SCHEMA = (
    "row_type string, record_key string, cluster_id string, record_key_a string, "
    "record_key_b string, tier long, conflict_record_key_a string, "
    "conflict_record_key_b string, conflict_reason_code string, reason_code string"
)
RESULT_ARROW_SCHEMA = pa.schema(
    [
        (name, pa.int64() if name == "tier" else pa.string())
        for name in (c.split(" ")[0] for c in RESULT_SCHEMA.split(", "))
    ]
)


def connected_components(edges: DataFrame, max_iterations: int = 1000) -> DataFrame:
    """(record_key, component) for every record in at least one edge."""
    links = edges.select(F.col("record_key_a").alias("src"), F.col("record_key_b").alias("dst"))
    links = links.union(links.select(F.col("dst").alias("src"), F.col("src").alias("dst")))
    labels = (
        links.select(F.col("src").alias("record_key"))
        .distinct()
        .withColumn("component", F.col("record_key"))
    )
    labels = labels.localCheckpoint()
    for _ in range(max_iterations):
        offered = links.join(labels.withColumnRenamed("record_key", "dst"), "dst").select(
            F.col("src").alias("record_key"), "component"
        )
        updated = (
            labels.union(offered)
            .groupBy("record_key")
            .agg(F.min("component").alias("component"))
            .localCheckpoint()
        )
        changed = updated.join(labels.withColumnRenamed("component", "old"), "record_key").where(
            F.col("component") < F.col("old")
        )
        labels = updated
        if changed.isEmpty():
            return labels
    raise RuntimeError(f"Connected components did not converge in {max_iterations} iterations")


def _cluster_one_component(rows: pa.Table) -> pa.Table:
    """applyInArrow body: one component's rows in, clusters and skipped joins out.

    Arrow in and out, with an explicit schema, so no pandas dtype inference is involved.
    """
    rows = rows.to_pylist()
    result = cluster_component(
        [r["record_key_a"] for r in rows if r["kind"] == "member"],
        [
            MatchEdge(r["record_key_a"], r["record_key_b"], r["tier"])
            for r in rows
            if r["kind"] == "edge"
        ],
        [
            CannotLink(r["record_key_a"], r["record_key_b"], r["reason_code"])
            for r in rows
            if r["kind"] == "link"
        ],
    )
    out = [
        {"row_type": "cluster", "record_key": key, "cluster_id": cluster}
        for key, cluster in result.cluster_of.items()
    ]
    out += [
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
    return pa.Table.from_pylist(out, RESULT_ARROW_SCHEMA)


def cluster(records: DataFrame, pairs: DataFrame) -> tuple[DataFrame, DataFrame]:
    """(clusters, skipped_joins) with the same columns as the dbt models."""
    edges = pairs.where("is_match").select("record_key_a", "record_key_b", "tier")
    components = connected_components(edges)

    comp_a = components.select(
        F.col("record_key").alias("record_key_a"), F.col("component").alias("component")
    )
    comp_b = components.select(
        F.col("record_key").alias("record_key_b"), F.col("component").alias("component_b")
    )
    links = (
        pairs.where("cannot_link")
        .select("record_key_a", "record_key_b", "reason_code")
        .join(comp_a, "record_key_a")
        .join(comp_b, "record_key_b")
        .where(F.col("component") == F.col("component_b"))
    )
    null = F.lit(None)
    grouped = (
        components.select(
            "component",
            F.lit("member").alias("kind"),
            F.col("record_key").alias("record_key_a"),
            null.cast("string").alias("record_key_b"),
            null.cast("int").alias("tier"),
            null.cast("string").alias("reason_code"),
        )
        .unionByName(
            edges.join(comp_a, "record_key_a").select(
                "component",
                F.lit("edge").alias("kind"),
                "record_key_a",
                "record_key_b",
                F.col("tier").cast("int"),
                null.cast("string").alias("reason_code"),
            )
        )
        .unionByName(
            links.select(
                "component",
                F.lit("link").alias("kind"),
                "record_key_a",
                "record_key_b",
                null.cast("int").alias("tier"),
                "reason_code",
            )
        )
    )
    results = grouped.groupBy("component").applyInArrow(_cluster_one_component, RESULT_SCHEMA)
    results = results.localCheckpoint()

    in_components = results.where("row_type = 'cluster'").select("record_key", "cluster_id")
    singletons = (
        records.select("record_key")
        .join(components.select("record_key"), "record_key", "left_anti")
        .withColumn("cluster_id", F.col("record_key"))
    )
    clusters = in_components.unionByName(singletons)
    sizes = clusters.groupBy("cluster_id").agg(F.count("*").alias("cluster_size"))
    clusters = clusters.join(sizes, "cluster_id").select("record_key", "cluster_id", "cluster_size")
    skipped = results.where("row_type = 'skipped'").select(
        "record_key_a",
        "record_key_b",
        "tier",
        "reason_code",
        "conflict_record_key_a",
        "conflict_record_key_b",
        "conflict_reason_code",
    )
    return clusters, skipped
