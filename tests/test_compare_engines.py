"""The engine comparison behind ``compare-engines`` and the parity section (D-045)."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

from evaluation.report import _parity_section
from pipeline import paths
from pipeline.compare_engines import compare_engines, read_summary, write_summary
from pipeline.runs import OUTPUT_TABLES

ROWS = [{"record_key": "A|1", "cluster_id": "A|1"}, {"record_key": "A|2", "cluster_id": "A|1"}]


def _write_run(data_dir, engine, rows, nullable=True, profile="conservative"):
    fields = [pa.field(name, pa.string(), nullable=nullable) for name in rows[0]]
    out = paths.matched_dir(engine, profile, 0, data_dir)
    out.mkdir(parents=True)
    for table in OUTPUT_TABLES:
        pq.write_table(pa.Table.from_pylist(rows, pa.schema(fields)), out / f"{table}.parquet")


def test_identical_runs_agree_whatever_their_row_order_and_nullability(tmp_path):
    _write_run(tmp_path, "dbt", ROWS, nullable=False)
    _write_run(tmp_path, "spark", list(reversed(ROWS)))
    result = compare_engines("conservative", tmp_path)
    assert result.identical
    assert result.tables_compared == len(OUTPUT_TABLES)
    assert result.rows_compared == 2 * len(OUTPUT_TABLES)


def test_a_differing_row_is_reported_from_both_sides(tmp_path):
    _write_run(tmp_path, "dbt", ROWS)
    _write_run(tmp_path, "spark", [ROWS[0], {"record_key": "A|2", "cluster_id": "A|2"}])
    result = compare_engines("conservative", tmp_path)
    assert len(result.differences) == len(OUTPUT_TABLES)
    difference = result.differences[0]
    assert difference.only_dbt == ({"record_key": "A|2", "cluster_id": "A|1"},)
    assert difference.only_spark == ({"record_key": "A|2", "cluster_id": "A|2"},)


def test_a_missing_engine_output_is_a_difference(tmp_path):
    _write_run(tmp_path, "dbt", ROWS)
    result = compare_engines("conservative", tmp_path)
    assert not result.identical
    assert "missing file" in result.differences[0].problem


def test_the_parity_section_reports_each_profile(tmp_path):
    for profile in ("conservative", "balanced"):
        _write_run(tmp_path, "dbt", ROWS, profile=profile)
        _write_run(tmp_path, "spark", ROWS, profile=profile)
        write_summary(compare_engines(profile, tmp_path), tmp_path)
    assert read_summary("conservative", tmp_path)["tables_differing"] == 0
    text = "\n".join(_parity_section(["conservative", "balanced"], tmp_path))
    assert "| conservative | 0-0 |" in text and "identical" in text
    assert "not compared" in "\n".join(_parity_section(["aggressive"], tmp_path))
