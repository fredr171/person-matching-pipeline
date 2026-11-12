"""Jaro-Winkler and Double Metaphone as pandas UDFs (DESIGN.md section 10, D-009).

The same functions back the DuckDB UDFs used by dbt
(``standardization/duckdb_udfs.py``), so both engines score names identically.

Python tasks are expensive on Windows: without ``fork`` PySpark cannot keep a
daemon of reusable workers, so every task that runs Python starts a fresh
interpreter (about three seconds). ``for_python`` therefore repartitions a
DataFrame to the fewest tasks its size needs before a Python step (D-046).
"""

from __future__ import annotations

import math
import os

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, DoubleType, StringType

from standardization.similarity import double_metaphone, jaro_winkler


@pandas_udf(DoubleType())
def jaro_winkler_udf(a: pd.Series, b: pd.Series) -> pd.Series:
    return pd.Series([jaro_winkler(x, y) for x, y in zip(a, b, strict=True)], dtype="float64")


@pandas_udf(ArrayType(StringType()))
def double_metaphone_udf(names: pd.Series) -> pd.Series:
    return pd.Series([list(double_metaphone(name)) for name in names])


ROWS_PER_PYTHON_TASK = int(os.environ.get("PMP_SPARK_ROWS_PER_PYTHON_TASK", "250000"))


def for_python(df: DataFrame) -> DataFrame:
    """``df`` materialized in about one partition per ROWS_PER_PYTHON_TASK rows.

    Capped at the session's parallelism, so large loads still use every core.
    """
    df = df.localCheckpoint()
    cores = df.sparkSession.sparkContext.defaultParallelism
    tasks = min(cores, max(1, math.ceil(df.count() / ROWS_PER_PYTHON_TASK)))
    return df.repartition(tasks)
