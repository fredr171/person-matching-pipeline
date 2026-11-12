"""Run the PySpark matching implementation load by load (build phase 7).

The counterpart of ``pipeline/match_dbt.py``: same inputs, same parameters
from ``config/matching.yaml``, same outputs under
``data/matched/spark/<profile>/load_NNNN/``. One Spark session serves every
load of a run, because starting Spark takes several seconds.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from datetime import date
from pathlib import Path

from pipeline import config, paths, reference
from pipeline.runs import (
    LoadRun,
    available_ingested_loads,
    current_records,
    load_date,
    previous_output_dir,
)

ENGINE = "spark"


def pair_parameters(matching: config.MatchingConfig, profile: config.Profile):
    from matching_spark.pairs import PairParameters

    return PairParameters(
        profile=profile.name,
        strong_name_threshold=profile.strong_name_threshold,
        weak_name_threshold=profile.weak_name_threshold,
        tier_4_matches=profile.tier_4_matches,
        prefix_min_length=matching.name_prefix_min_length,
        generational_gap_years=matching.generational_gap_years,
        newborn_max_age_years=matching.newborn_max_age_years,
        max_block_size=matching.max_block_size,
        blocking_keys=tuple(matching.blocking_keys),
    )


def match_load(
    load_id: int,
    data_dir: Path = paths.DEFAULT_DATA_DIR,
    profile_name: str | None = None,
    matching: config.MatchingConfig | None = None,
    spark=None,
) -> LoadRun:
    """Match one load with Spark; the previous load must already be matched."""
    from matching_spark.run import LoadInputs
    from matching_spark.run import match_load as run_load
    from matching_spark.session import spark_session

    matching = matching or config.load_matching_config()
    profile = matching.profile(profile_name)
    output_dir = paths.matched_dir(ENGINE, profile.name, load_id, data_dir)
    inputs = LoadInputs(
        load_id=load_id,
        load_date=date.fromisoformat(load_date(load_id, data_dir)),
        current_records=current_records(load_id, data_dir),
        previous_output_dir=previous_output_dir(ENGINE, profile.name, load_id, data_dir),
        output_dir=output_dir,
        params=pair_parameters(matching, profile),
        person_id=matching.person_id,
    )
    started = time.perf_counter()
    tables = run_load(spark or spark_session(), reference.load_reference_data(), inputs)
    metrics = {
        (row["metric_group"], row["metric"]): row["value"]
        for row in tables["run_metrics"].to_pylist()
    }
    return LoadRun(load_id, output_dir, time.perf_counter() - started, metrics)


def match_all(
    data_dir: Path = paths.DEFAULT_DATA_DIR,
    profile_name: str | None = None,
    through_load: int | None = None,
) -> Iterator[LoadRun]:
    """Match every ingested load in order, from load 0; yields one LoadRun per load."""
    from matching_spark.session import spark_session

    matching = config.load_matching_config()
    loads = available_ingested_loads(data_dir, through_load)
    spark = spark_session()
    for load_id in loads:
        yield match_load(load_id, data_dir, profile_name, matching, spark)
