"""Check that the dbt and PySpark runs produced the same outputs (build phase 7, D-045).

Every output table of every load is compared row for row: same column names
and types, same rows. Nullability flags are not compared: DuckDB marks columns
NOT NULL where it can prove it and Spark does not, and the values themselves
are compared anyway. Both engines write rows in a canonical order (sorted by every non-list
column), but the comparison sorts again so it does not depend on that.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from pipeline import paths
from pipeline.runs import OUTPUT_TABLES

ENGINES = ("dbt", "spark")


@dataclass(frozen=True)
class TableDifference:
    load_id: int
    table: str
    problem: str
    only_dbt: tuple[dict, ...] = ()
    only_spark: tuple[dict, ...] = ()


@dataclass
class Comparison:
    profile: str
    loads: list[int] = field(default_factory=list)
    tables_compared: int = 0
    rows_compared: int = 0
    differences: list[TableDifference] = field(default_factory=list)

    @property
    def identical(self) -> bool:
        return not self.differences


def _columns(schema: pa.Schema) -> list[tuple[str, str]]:
    return [(f.name, str(f.type)) for f in schema]


def _canonical_rows(table: pa.Table) -> list[dict]:
    keys = [(f.name, "ascending") for f in table.schema if not pa.types.is_list(f.type)]
    return table.sort_by(keys).to_pylist()


def _row_key(row: dict) -> tuple:
    return tuple((k, tuple(v) if isinstance(v, list) else v) for k, v in row.items())


def compare_table(
    dbt_path: Path, spark_path: Path, load_id: int, table: str, examples: int = 5
) -> TableDifference | None:
    for path in (dbt_path, spark_path):
        if not path.is_file():
            return TableDifference(load_id, table, f"missing file {path}")
    dbt_table, spark_table = pq.read_table(dbt_path), pq.read_table(spark_path)
    if _columns(dbt_table.schema) != _columns(spark_table.schema):
        return TableDifference(
            load_id,
            table,
            f"columns differ: dbt {_columns(dbt_table.schema)}, "
            f"spark {_columns(spark_table.schema)}",
        )
    dbt_rows, spark_rows = _canonical_rows(dbt_table), _canonical_rows(spark_table)
    if dbt_rows == spark_rows:
        return None
    dbt_keys = {_row_key(r) for r in dbt_rows}
    spark_keys = {_row_key(r) for r in spark_rows}
    only_dbt = [r for r in dbt_rows if _row_key(r) not in spark_keys]
    only_spark = [r for r in spark_rows if _row_key(r) not in dbt_keys]
    return TableDifference(
        load_id,
        table,
        f"{len(dbt_rows)} dbt rows vs {len(spark_rows)} spark rows; "
        f"{len(only_dbt)} only in dbt, {len(only_spark)} only in spark",
        tuple(only_dbt[:examples]),
        tuple(only_spark[:examples]),
    )


def matched_loads(engine: str, profile: str, data_dir: Path) -> list[int]:
    root = paths.matched_dir(engine, profile, 0, data_dir).parent
    return sorted(int(p.name.split("_")[1]) for p in root.glob("load_*") if p.is_dir())


def compare_engines(
    profile: str, data_dir: Path = paths.DEFAULT_DATA_DIR, through_load: int | None = None
) -> Comparison:
    """Compare every output table of every load both engines have matched."""
    dbt_loads = set(matched_loads("dbt", profile, data_dir))
    spark_loads = set(matched_loads("spark", profile, data_dir))
    loads = sorted(dbt_loads | spark_loads)
    if through_load is not None:
        loads = [load for load in loads if load <= through_load]
    if not loads:
        raise FileNotFoundError(f"Neither engine has matched profile {profile} under {data_dir}")
    comparison = Comparison(profile, loads)
    for load_id in loads:
        dirs = {e: paths.matched_dir(e, profile, load_id, data_dir) for e in ENGINES}
        for table in OUTPUT_TABLES:
            difference = compare_table(
                dirs["dbt"] / f"{table}.parquet", dirs["spark"] / f"{table}.parquet", load_id, table
            )
            comparison.tables_compared += 1
            if difference is None:
                comparison.rows_compared += pq.read_metadata(
                    dirs["dbt"] / f"{table}.parquet"
                ).num_rows
            else:
                comparison.differences.append(difference)
    return comparison
