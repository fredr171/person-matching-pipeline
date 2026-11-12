"""A local Spark session for the matching run (D-011, D-044).

Spark runs in local mode: no cluster and no cloud account. The driver binds to
127.0.0.1 because Spark rejects machine hostnames containing underscores, which
Windows produces. Python workers use the interpreter running the pipeline, so
they import the same ``standardization`` and ``clustering`` packages.

Tunables, all optional environment variables:

* ``PMP_SPARK_MASTER`` (default ``local[*]``)
* ``PMP_SPARK_DRIVER_MEMORY`` (default ``4g``)
* ``PMP_SPARK_SHUFFLE_PARTITIONS`` (default ``16``)
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession


def spark_session(app_name: str = "person-matching") -> SparkSession:
    """Create (or reuse) the local session used by every load of a run."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("PMP_SPARK_MASTER", "local[*]"))
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.memory", os.environ.get("PMP_SPARK_DRIVER_MEMORY", "4g"))
        .config(
            "spark.sql.shuffle.partitions", os.environ.get("PMP_SPARK_SHUFFLE_PARTITIONS", "16")
        )
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
