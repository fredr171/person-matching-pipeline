"""One load of the PySpark matching run (build phase 7).

Spark computes standardization, blocking, comparison, tiers, blockers and
clustering. The driver then reconciles person IDs with the shared Python
function, exactly as dbt's Python model does, and writes every table with
pyarrow in the same layout and schema as the dbt run (D-044): Spark cannot
write files on native Windows without Hadoop binaries (D-011), and one file
per table lets evaluation and the parity check read both engines the same way.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from clustering.ids import reconcile
from matching_spark import cluster, pairs, standardize
from pipeline.config import PersonIdFormat
from pipeline.reference import ReferenceData

STRING, INT32, INT64 = pa.string(), pa.int32(), pa.int64()
STRINGS = pa.list_(pa.string())

# Identical to the dbt outputs (matching_dbt/models/outputs/).
SCHEMAS: dict[str, pa.Schema] = {
    "records_standardized": pa.schema(
        [
            ("source_system", STRING),
            ("source_record_id", STRING),
            ("record_life", INT64),
            ("record_key", STRING),
            ("record_uid", STRING),
            ("first_name_raw", STRING),
            ("last_name_raw", STRING),
            ("dob_raw", STRING),
            ("ssn_raw", STRING),
            ("gender_raw", STRING),
            ("first_name", STRING),
            ("last_name", STRING),
            ("first_name_forms", STRINGS),
            ("last_name_forms", STRINGS),
            ("first_name_is_placeholder", pa.bool_()),
            ("last_name_is_placeholder", pa.bool_()),
            ("suffix", STRING),
            ("ssn", STRING),
            ("ssn_null_reason", STRING),
            ("dob", pa.date32()),
            ("dob_null_reason", STRING),
            ("dob_is_jan1", pa.bool_()),
            ("gender", STRING),
        ]
    ),
    "match_pairs": pa.schema(
        [
            ("load_id", INT32),
            ("profile", STRING),
            ("record_key_a", STRING),
            ("record_key_b", STRING),
            ("blocking_keys", STRINGS),
            ("ssn_level", STRING),
            ("dob_level", STRING),
            ("dob_gap_years", INT64),
            ("first_level", STRING),
            ("first_method", STRING),
            ("last_level", STRING),
            ("last_method", STRING),
            ("names_swapped", pa.bool_()),
            ("tier", INT32),
            ("blockers", STRINGS),
            ("reason_code", STRING),
            ("is_match", pa.bool_()),
            ("cannot_link", pa.bool_()),
        ]
    ),
    "clusters": pa.schema(
        [("record_key", STRING), ("cluster_id", STRING), ("cluster_size", INT64)]
    ),
    "skipped_joins": pa.schema(
        [
            ("load_id", INT32),
            ("record_key_a", STRING),
            ("record_key_b", STRING),
            ("tier", INT64),
            ("reason_code", STRING),
            ("conflict_record_key_a", STRING),
            ("conflict_record_key_b", STRING),
            ("conflict_reason_code", STRING),
        ]
    ),
    "skipped_blocks": pa.schema(
        [("key_name", STRING), ("key_value", STRING), ("block_size", INT64)]
    ),
    "person_assignment": pa.schema(
        [
            ("load_id", INT32),
            ("source_system", STRING),
            ("source_record_id", STRING),
            ("record_life", INT64),
            ("record_key", STRING),
            ("record_uid", STRING),
            ("person_id", STRING),
            ("cluster_id", STRING),
        ]
    ),
    "person_id_crosswalk": pa.schema(
        [("load_id", INT32), ("retired_id", STRING), ("successor_id", STRING), ("reason", STRING)]
    ),
    "id_state": pa.schema([("next_sequence", INT64)]),
    "run_metrics": pa.schema([("metric_group", STRING), ("metric", STRING), ("value", INT64)]),
}


@dataclass(frozen=True)
class LoadInputs:
    load_id: int
    load_date: date
    current_records: Path
    previous_output_dir: Path | None
    output_dir: Path
    params: pairs.PairParameters
    person_id: PersonIdFormat


def _to_arrow(df: DataFrame, name: str) -> pa.Table:
    return df.toArrow().select(SCHEMAS[name].names).cast(SCHEMAS[name])


def write_table(table: pa.Table, name: str, output_dir: Path) -> None:
    """Write one output, rows sorted by every non-list column for a stable file."""
    table = table.select(SCHEMAS[name].names).cast(SCHEMAS[name])
    keys = [(f.name, "ascending") for f in SCHEMAS[name] if not pa.types.is_list(f.type)]
    pq.write_table(table.sort_by(keys), output_dir / f"{name}.parquet")


def _previous(inputs: LoadInputs) -> tuple[dict[str, str], int]:
    if inputs.previous_output_dir is None:
        return {}, 1
    assignment = pq.read_table(
        inputs.previous_output_dir / "person_assignment.parquet",
        columns=["record_uid", "person_id"],
    ).to_pydict()
    previous = dict(zip(assignment["record_uid"], assignment["person_id"], strict=True))
    state = pq.read_table(inputs.previous_output_dir / "id_state.parquet").to_pydict()
    return previous, int(state["next_sequence"][0])


def run_metrics(tables: dict[str, pa.Table], previous: dict[str, str]) -> pa.Table:
    """The same (metric_group, metric, value) rows as dbt's run_metrics model."""
    match = tables["match_pairs"].to_pydict()
    blocks = tables["skipped_blocks"].to_pydict()
    clusters = tables["clusters"].to_pydict()
    assignment = tables["person_assignment"].to_pydict()
    crosswalk = tables["person_id_crosswalk"].to_pydict()

    rows: list[tuple[str, str, int]] = [
        ("records", "live_records", tables["records_standardized"].num_rows),
        ("pairs", "compared", len(match["record_key_a"])),
        ("pairs", "matched", sum(match["is_match"])),
        ("pairs", "cannot_link", sum(match["cannot_link"])),
    ]
    rows += [("reason_code", k, v) for k, v in Counter(match["reason_code"]).items()]
    rows += [("skipped_blocks", k, v) for k, v in Counter(blocks["key_name"]).items()]
    rows.append(("skipped_blocks", "records_in_skipped_blocks", sum(blocks["block_size"])))
    rows.append(("clustering", "SKIPPED_TRANSITIVITY", tables["skipped_joins"].num_rows))
    cluster_sizes = dict(zip(clusters["cluster_id"], clusters["cluster_size"], strict=True))
    rows.append(("clustering", "clusters", len(cluster_sizes)))
    rows += [("cluster_size", str(size), n) for size, n in Counter(cluster_sizes.values()).items()]
    previous_ids = set(previous.values())
    rows.append(("person_ids", "new", len(set(assignment["person_id"]) - previous_ids)))
    rows += [
        ("person_ids", f"retired_{reason.lower()}", n)
        for reason, n in Counter(crosswalk["reason"]).items()
    ]
    rows.append(
        (
            "person_ids",
            "records_with_changed_id",
            sum(
                1
                for uid, pid in zip(assignment["record_uid"], assignment["person_id"], strict=True)
                if uid in previous and previous[uid] != pid
            ),
        )
    )
    names = ("metric_group", "metric", "value")
    return pa.Table.from_pylist(
        [dict(zip(names, row, strict=True)) for row in rows], SCHEMAS["run_metrics"]
    )


def match_load(spark: SparkSession, ref: ReferenceData, inputs: LoadInputs) -> dict[str, pa.Table]:
    """Match one load and write its outputs; returns the written tables."""
    params, load_id = inputs.params, inputs.load_id
    raw = spark.read.parquet(inputs.current_records.as_posix())
    records = standardize.standardize_records(raw, ref, inputs.load_date).localCheckpoint()

    codes = pairs.name_codes(records)
    keys = pairs.block_keys(records, codes, params.blocking_keys).localCheckpoint()
    skipped_blocks = pairs.skipped_blocks(keys, params.max_block_size)
    candidates = pairs.candidate_pairs(keys, skipped_blocks).localCheckpoint()
    form_pairs = pairs.name_form_pairs(candidates, records).localCheckpoint()
    name_keys, ambiguous = pairs.name_keys_and_ambiguity(spark, ref)
    scores = pairs.form_scores(form_pairs, codes, name_keys, ambiguous)
    levels = pairs.name_levels(candidates, records, form_pairs, scores, params)
    match = pairs.match_pairs(candidates, records, levels, params, load_id, inputs.load_date)
    match = match.localCheckpoint()
    clusters, skipped = cluster.cluster(records, match)

    tables = {
        "records_standardized": _to_arrow(records, "records_standardized"),
        "match_pairs": _to_arrow(match, "match_pairs"),
        "clusters": _to_arrow(clusters, "clusters"),
        "skipped_joins": _to_arrow(
            skipped.withColumn("load_id", F.lit(load_id).cast("int")), "skipped_joins"
        ),
        "skipped_blocks": _to_arrow(skipped_blocks, "skipped_blocks"),
    }

    # Stable IDs (section 11), on the driver with the shared function.
    previous, next_sequence = _previous(inputs)
    rs = tables["records_standardized"].to_pydict()
    cluster_of = dict(
        zip(
            tables["clusters"]["record_key"].to_pylist(),
            tables["clusters"]["cluster_id"].to_pylist(),
            strict=True,
        )
    )
    result = reconcile(
        {uid: cluster_of[key] for key, uid in zip(rs["record_key"], rs["record_uid"], strict=True)},
        previous,
        next_sequence,
        inputs.person_id,
    )
    tables["person_assignment"] = pa.Table.from_pylist(
        [
            {
                "load_id": load_id,
                "source_system": system,
                "source_record_id": record_id,
                "record_life": life,
                "record_key": key,
                "record_uid": uid,
                "person_id": result.person_of[uid],
                "cluster_id": cluster_of[key],
            }
            for system, record_id, life, key, uid in zip(
                rs["source_system"],
                rs["source_record_id"],
                rs["record_life"],
                rs["record_key"],
                rs["record_uid"],
                strict=True,
            )
        ],
        SCHEMAS["person_assignment"],
    )
    tables["person_id_crosswalk"] = pa.Table.from_pylist(
        [
            {
                "load_id": load_id,
                "retired_id": r.retired_id,
                "successor_id": r.successor_id,
                "reason": r.reason,
            }
            for r in result.retirements
        ],
        SCHEMAS["person_id_crosswalk"],
    )
    tables["id_state"] = pa.table({"next_sequence": [result.next_sequence]}, SCHEMAS["id_state"])
    tables["run_metrics"] = run_metrics(tables, previous)

    inputs.output_dir.mkdir(parents=True, exist_ok=True)
    for name, table in tables.items():
        write_table(table, name, inputs.output_dir)
    return tables
