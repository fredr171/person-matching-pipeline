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
from pathlib import Path

from pipeline import config, paths
from pipeline.runs import (
    OUTPUT_TABLES,  # noqa: F401 - re-exported for callers of this module
    LoadRun,
    available_ingested_loads,
    current_records,
    load_date,
    previous_output_dir,
    read_metrics,
)

ENGINE = "dbt"


class DbtRunError(RuntimeError):
    """Raised when dbt fails for a load."""


def dbt_vars(
    load_id: int, data_dir: Path, matching: config.MatchingConfig, profile: config.Profile
) -> dict:
    """The dbt vars for one load: inputs, outputs and every profile parameter."""
    current = current_records(load_id, data_dir)
    previous_dir = previous_output_dir(ENGINE, profile.name, load_id, data_dir)
    previous = previous_dir.as_posix() if previous_dir else ""
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


def match_all(
    data_dir: Path = paths.DEFAULT_DATA_DIR,
    profile_name: str | None = None,
    through_load: int | None = None,
):
    """Match every ingested load in order, from load 0; yields one LoadRun per load."""
    matching = config.load_matching_config()
    for load_id in available_ingested_loads(data_dir, through_load):
        yield match_load(load_id, data_dir, profile_name, matching)
