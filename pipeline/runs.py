"""What the dbt and PySpark runners share: inputs, previous outputs and results.

Both engines match one ingested load at a time, read the previous load's
outputs of the same engine and profile for ID reconciliation, and write the
same tables to ``data/matched/<engine>/<profile>/load_NNNN/`` (D-042, D-044).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from pipeline import paths

OUTPUT_TABLES = (
    "records_standardized",
    "match_pairs",
    "clusters",
    "skipped_joins",
    "skipped_blocks",
    "person_assignment",
    "person_id_crosswalk",
    "id_state",
    "run_metrics",
)


@dataclass(frozen=True)
class LoadRun:
    load_id: int
    output_dir: Path
    seconds: float
    metrics: dict[tuple[str, str], int]


def load_date(load_id: int, data_dir: Path) -> str:
    """The load's date from its manifest: what "today" means for this load (D-026)."""
    manifest = paths.load_dir(load_id, data_dir) / "manifest.json"
    return json.loads(manifest.read_text(encoding="utf-8"))["load_date"]


def current_records(load_id: int, data_dir: Path) -> Path:
    path = paths.ingested_dir(load_id, data_dir) / "current_records.parquet"
    if not path.is_file():
        raise FileNotFoundError(f"Load {load_id} has not been ingested ({path}); run `ingest`")
    return path


def previous_output_dir(engine: str, profile: str, load_id: int, data_dir: Path) -> Path | None:
    """The previous load's outputs for ID reconciliation; None for load 0."""
    if load_id == 0:
        return None
    previous = paths.matched_dir(engine, profile, load_id - 1, data_dir)
    if not (previous / "person_assignment.parquet").is_file():
        raise FileNotFoundError(
            f"Load {load_id - 1} has not been matched with {engine}, profile {profile} "
            f"({previous}); match earlier loads first"
        )
    return previous


def available_ingested_loads(data_dir: Path, through_load: int | None = None) -> list[int]:
    root = data_dir / "ingested"
    loads = sorted(int(p.name.split("_")[1]) for p in root.glob("load_*") if p.is_dir())
    if not loads:
        raise FileNotFoundError(f"No ingested loads under {root}; run `ingest`")
    return [load for load in loads if through_load is None or load <= through_load]


def read_metrics(output_dir: Path) -> dict[tuple[str, str], int]:
    import pyarrow.parquet as pq

    table = pq.read_table(output_dir / "run_metrics.parquet").to_pylist()
    return {(row["metric_group"], row["metric"]): row["value"] for row in table}
