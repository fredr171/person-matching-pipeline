"""Run the dbt-duckdb matching implementation load by load (build phase 5).

For each load N this runs ``dbt build`` with:

* the live records from ingestion (``data/ingested/load_NNNN/current_records.parquet``),
* the load date from the load manifest ("today" for future-DOB and newborn rules, D-026),
* every threshold and parameter of the chosen profile from ``config/matching.yaml``,
* the previous load's outputs for this profile, for ID reconciliation.

Outputs go to ``data/matched/dbt/<profile>/load_NNNN/`` (DESIGN.md section 13).
Matching recomputes every pair each load (D-004); only person IDs carry over.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path

from pipeline import config, paths

ENGINE = "dbt"
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


class DbtRunError(RuntimeError):
    """Raised when dbt fails for a load."""


@dataclass(frozen=True)
class LoadRun:
    load_id: int
    output_dir: Path
    seconds: float
    metrics: dict[tuple[str, str], int]


def load_date(load_id: int, data_dir: Path) -> str:
    manifest = paths.load_dir(load_id, data_dir) / "manifest.json"
    return json.loads(manifest.read_text(encoding="utf-8"))["load_date"]


def dbt_vars(
    load_id: int, data_dir: Path, matching: config.MatchingConfig, profile: config.Profile
) -> dict:
    """The dbt vars for one load: inputs, outputs and every profile parameter."""
    current = paths.ingested_dir(load_id, data_dir) / "current_records.parquet"
    if not current.is_file():
        raise FileNotFoundError(f"Load {load_id} has not been ingested ({current}); run `ingest`")
    previous = ""
    if load_id > 0:
        previous_dir = paths.matched_dir(ENGINE, profile.name, load_id - 1, data_dir)
        if not (previous_dir / "person_assignment.parquet").is_file():
            raise FileNotFoundError(
                f"Load {load_id - 1} has not been matched with profile {profile.name} "
                f"({previous_dir}); match earlier loads first"
            )
        previous = previous_dir.as_posix()
    return {
        "load_id": load_id,
        "load_date": load_date(load_id, data_dir),
        "current_records_path": current.as_posix(),
        "previous_output_dir": previous,
        "profile": profile.name,
        "strong_name_threshold": profile.strong_name_threshold,
        "weak_name_threshold": profile.weak_name_threshold,
        "tier_4_matches": profile.tier_4_matches,
        "prefix_min_length": matching.name_prefix_min_length,
        "generational_gap_years": matching.generational_gap_years,
        "newborn_max_age_years": matching.newborn_max_age_years,
        "max_block_size": matching.max_block_size,
        "blocking_keys": list(matching.blocking_keys),
        "person_id_prefix": matching.person_id.prefix,
        "person_id_digits": matching.person_id.digits,
        "output_dir": paths.matched_dir(ENGINE, profile.name, load_id, data_dir).as_posix(),
    }


def run_dbt(args: list[str], warehouse: Path) -> None:
    """Invoke dbt in-process against ``warehouse`` and the repo's reference data."""
    from dbt.cli.main import dbtRunner

    os.environ["PMP_DUCKDB_PATH"] = warehouse.as_posix()
    os.environ["PMP_REFERENCE_DIR"] = paths.REFERENCE_DIR.as_posix()
    os.environ.setdefault("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
    project = str(paths.DBT_PROJECT_DIR)
    result = dbtRunner().invoke(
        [*args, "--project-dir", project, "--profiles-dir", project, "--quiet"]
    )
    if not result.success:
        detail = result.exception or "see matching_dbt/logs/dbt.log"
        raise DbtRunError(f"dbt {' '.join(args[:1])} failed: {detail}")


def read_metrics(output_dir: Path) -> dict[tuple[str, str], int]:
    import pyarrow.parquet as pq

    table = pq.read_table(output_dir / "run_metrics.parquet").to_pylist()
    return {(row["metric_group"], row["metric"]): row["value"] for row in table}


def match_load(
    load_id: int,
    data_dir: Path = paths.DEFAULT_DATA_DIR,
    profile_name: str | None = None,
    matching: config.MatchingConfig | None = None,
) -> LoadRun:
    """Run dbt for one load; the previous load must already be matched."""
    matching = matching or config.load_matching_config()
    profile = matching.profile(profile_name)
    variables = dbt_vars(load_id, data_dir, matching, profile)
    output_dir = Path(variables["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)
    warehouse = paths.warehouse_path(ENGINE, profile.name, data_dir)
    warehouse.parent.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    run_dbt(["build", "--vars", json.dumps(variables)], warehouse)
    return LoadRun(load_id, output_dir, time.perf_counter() - started, read_metrics(output_dir))


def available_ingested_loads(data_dir: Path) -> list[int]:
    root = data_dir / "ingested"
    return sorted(int(p.name.split("_")[1]) for p in root.glob("load_*") if p.is_dir())


def match_all(
    data_dir: Path = paths.DEFAULT_DATA_DIR,
    profile_name: str | None = None,
    through_load: int | None = None,
):
    """Match every ingested load in order, from load 0; yields one LoadRun per load."""
    matching = config.load_matching_config()
    loads = available_ingested_loads(data_dir)
    if not loads:
        raise FileNotFoundError(f"No ingested loads under {data_dir / 'ingested'}; run `ingest`")
    if through_load is not None:
        loads = [load for load in loads if load <= through_load]
    for load_id in loads:
        yield match_load(load_id, data_dir, profile_name, matching)
