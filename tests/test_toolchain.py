"""Smoke tests: the pinned toolchain works together on this machine.

These start dbt and Spark, so they are slower than the unit tests and carry
the ``toolchain`` marker (deselect with ``pytest -m "not toolchain"``).

They also prove the first half of the "one copy of reference data" rule:
dbt reads ``config/reference/`` and sees exactly the rows the Python loader sees.
"""

from __future__ import annotations

import sys

import duckdb
import pytest

from pipeline import paths

pytestmark = pytest.mark.toolchain


@pytest.fixture
def dbt_env(tmp_path, monkeypatch):
    """Point dbt at a throwaway DuckDB file and the repo's reference data."""
    monkeypatch.setenv("PMP_DUCKDB_PATH", str(tmp_path / "smoke.duckdb"))
    monkeypatch.setenv("PMP_REFERENCE_DIR", paths.REFERENCE_DIR.as_posix())
    monkeypatch.setenv("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
    return tmp_path / "smoke.duckdb"


def _dbt(*args: str) -> None:
    from dbt.cli.main import dbtRunner

    project = str(paths.DBT_PROJECT_DIR)
    result = dbtRunner().invoke(
        [*args, "--project-dir", project, "--profiles-dir", project, "--quiet"]
    )
    assert result.success, f"dbt {' '.join(args)} failed: {result.exception}"


def test_dbt_reads_reference_data_identically(dbt_env, reference_data):
    _dbt("build", "--select", "stg_ref_*")

    # Same connection config as dbt-duckdb, which keeps its in-process connection open.
    with duckdb.connect(str(dbt_env)) as con:

        def rows(sql: str) -> set[tuple]:
            return set(con.execute(sql).fetchall())

        assert rows(
            "select canonical, canonical_gender, variant, relation from stg_ref_name_variants"
        ) == {
            (v.canonical, v.canonical_gender, v.variant, v.relation)
            for v in reference_data.name_variants
        }
        assert rows("select field, value, reason from stg_ref_placeholders") == {
            (p.field, p.value, p.reason) for p in reference_data.placeholders
        }
        assert rows("select particle from stg_ref_particles") == {
            (p,) for p in reference_data.particles
        }
        assert rows("select form, canonical from stg_ref_suffixes") == set(
            reference_data.suffixes.items()
        )
        assert rows('select "character", replacement from stg_ref_letters') == set(
            reference_data.letters.items()
        )
        assert rows("select value, gender from stg_ref_genders") == set(
            reference_data.genders.items()
        )


def test_spark_local_session_runs_apply_in_pandas(monkeypatch):
    """Spark starts locally and can run the applyInPandas pattern used for clustering.

    Also guards the pandas pin: PySpark 4.3 warns that it does not fully
    support pandas 3, which is why pandas is pinned to 2.x (D-019). The pytest
    config in pyproject.toml turns that warning into a failure.

    The driver is bound to 127.0.0.1 because Spark rejects machine hostnames
    containing underscores, which are common on Windows.
    """
    monkeypatch.setenv("PYSPARK_PYTHON", sys.executable)
    monkeypatch.setenv("PYSPARK_DRIVER_PYTHON", sys.executable)
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("toolchain-smoke")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    try:
        df = spark.createDataFrame([(1, "a"), (1, "b"), (2, "c")], ["component", "record_key"])

        def count_records(pdf):
            return pdf.groupby("component", as_index=False).agg(n=("record_key", "count"))

        result = (
            df.groupBy("component")
            .applyInPandas(count_records, schema="component long, n long")
            .orderBy("component")
            .collect()
        )
        assert [(r.component, r.n) for r in result] == [(1, 2), (2, 1)]
    finally:
        spark.stop()
