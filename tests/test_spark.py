"""The PySpark implementation produces exactly the dbt outputs (build phase 7).

``tests/test_dbt.py`` proves dbt reproduces the Python reference, so here every
output table of the Spark run is compared row for row with the dbt run of the
same data: the first loads of the boosted generated run, the hand-made load of
tricky raw values, and the aggressive profile. Then re-runs check idempotency
and ID stability, as for dbt.

These start Spark and dbt, so they carry the ``toolchain`` marker.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from tricky_values import write_tricky_load

from pipeline import match_dbt, match_spark, paths
from pipeline.compare_engines import compare_engines
from pipeline.runs import OUTPUT_TABLES

pytestmark = pytest.mark.toolchain

LOADS = 3  # loads of the boosted run matched with both engines


@pytest.fixture(scope="module")
def spark():
    from matching_spark.session import spark_session

    return spark_session("person-matching-tests")


def _match_both(data_dir: Path, loads, spark, profile: str | None = None) -> None:
    for load_id in loads:
        match_dbt.match_load(load_id, data_dir, profile)
        match_spark.match_load(load_id, data_dir, profile, spark=spark)


def _assert_identical(profile: str, data_dir: Path) -> None:
    result = compare_engines(profile, data_dir)
    assert result.identical, "\n".join(
        f"load {d.load_id} {d.table}: {d.problem}\n  dbt only: {d.only_dbt}\n"
        f"  spark only: {d.only_spark}"
        for d in result.differences
    )
    assert result.tables_compared == len(result.loads) * len(OUTPUT_TABLES)


@pytest.fixture(scope="module")
def boosted_both(ingested_boosted, tmp_path_factory, spark):
    """The first LOADS loads of the boosted run, matched with both engines."""
    source, _ = ingested_boosted
    data_dir = tmp_path_factory.mktemp("spark") / "data"
    for load_id in range(LOADS):
        for getter in (paths.load_dir, paths.ingested_dir):
            shutil.copytree(getter(load_id, source), getter(load_id, data_dir))
    _match_both(data_dir, range(LOADS), spark)
    return data_dir


def _read(output_dir: Path, table: str) -> set:
    rows = pq.read_table(output_dir / f"{table}.parquet").to_pylist()
    return {tuple((k, tuple(v) if isinstance(v, list) else v) for k, v in r.items()) for r in rows}


def test_generated_loads_are_identical_to_dbt(boosted_both):
    _assert_identical("conservative", boosted_both)


def test_generated_loads_exercise_every_stage(boosted_both):
    """Guards the parity test against comparing two empty runs."""
    last = paths.matched_dir("spark", "conservative", LOADS - 1, boosted_both)
    metrics = match_spark_metrics(last)
    assert metrics[("pairs", "matched")] > 0
    assert metrics[("pairs", "cannot_link")] > 0
    later = [
        match_spark_metrics(paths.matched_dir("spark", "conservative", load, boosted_both))
        for load in range(1, LOADS)
    ]
    assert any(m[("person_ids", "new")] > 0 for m in later)  # reconciliation ran on history
    assert any(group == "cluster_size" and int(size) > 2 for group, size in metrics)


def match_spark_metrics(output_dir: Path) -> dict[tuple[str, str], int]:
    rows = pq.read_table(output_dir / "run_metrics.parquet").to_pylist()
    return {(r["metric_group"], r["metric"]): r["value"] for r in rows}


def test_tricky_values_are_identical_to_dbt(tmp_path, spark):
    data_dir = tmp_path / "data"
    write_tricky_load(data_dir)
    _match_both(data_dir, [0], spark)
    _assert_identical("conservative", data_dir)
    reasons = {
        r
        for (group, r) in match_spark_metrics(
            paths.matched_dir("spark", "conservative", 0, data_dir)
        )
        if group == "reason_code"
    }
    assert {
        "TIER_1_EXACT_IDS",
        "TIER_2_SSN_NEAR",
        "TIER_3_DOB_NEAR",
        "BLOCKED_SUFFIX",
        "BLOCKED_TWINS",
        "BLOCKED_GENERATIONAL",
    } <= reasons


def test_aggressive_profile_is_identical_to_dbt(boosted_both, tmp_path, spark):
    """Profile thresholds and Tier 4 reach Spark from config/matching.yaml too."""
    data_dir = tmp_path / "data"
    for getter in (paths.load_dir, paths.ingested_dir):
        shutil.copytree(getter(0, boosted_both), getter(0, data_dir))
    _match_both(data_dir, [0], spark, "aggressive")
    _assert_identical("aggressive", data_dir)
    output = paths.matched_dir("spark", "aggressive", 0, data_dir)
    assert match_spark_metrics(output).get(("reason_code", "TIER_4_NO_SSN"), 0) > 0


def test_rerunning_a_load_reproduces_its_outputs(boosted_both, spark):
    output = paths.matched_dir("spark", "conservative", LOADS - 1, boosted_both)
    before = {table: _read(output, table) for table in OUTPUT_TABLES}
    match_spark.match_load(LOADS - 1, boosted_both, spark=spark)
    assert {table: _read(output, table) for table in OUTPUT_TABLES} == before


def test_matching_the_same_records_again_changes_no_person_id(boosted_both, tmp_path, spark):
    """A load identical to its predecessor keeps every person ID and retires none."""
    data_dir = tmp_path / "data"
    previous = paths.matched_dir("spark", "conservative", LOADS - 1, boosted_both)
    shutil.copytree(previous, paths.matched_dir("spark", "conservative", LOADS - 1, data_dir))
    for getter in (paths.load_dir, paths.ingested_dir):
        shutil.copytree(getter(LOADS - 1, boosted_both), getter(LOADS, data_dir))
    run = match_spark.match_load(LOADS, data_dir, spark=spark)
    before = {
        r["record_uid"]: r["person_id"]
        for r in pq.read_table(previous / "person_assignment.parquet").to_pylist()
    }
    after = {
        r["record_uid"]: r["person_id"]
        for r in pq.read_table(run.output_dir / "person_assignment.parquet").to_pylist()
    }
    assert after == before
    assert pq.read_table(run.output_dir / "person_id_crosswalk.parquet").num_rows == 0


def test_matching_a_load_before_its_predecessor_fails_clearly(tmp_path, boosted_both):
    target = tmp_path / "data"
    for getter in (paths.load_dir, paths.ingested_dir):
        shutil.copytree(getter(1, boosted_both), getter(1, target))
    with pytest.raises(FileNotFoundError, match="match earlier loads first"):
        match_spark.match_load(1, target)
