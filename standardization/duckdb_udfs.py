"""Register the shared similarity functions in DuckDB (D-009).

dbt-duckdb loads this module as a plugin (``matching_dbt/profiles.yml``) and
calls :meth:`Plugin.configure_connection` on every connection it opens, so
SQL models can call:

``pmp_jaro_winkler(a, b) -> DOUBLE``
    :func:`standardization.similarity.jaro_winkler`.
``pmp_double_metaphone(name) -> VARCHAR[]``
    :func:`standardization.similarity.double_metaphone`.

The functions are vectorized (Arrow) to keep per-row overhead low; each still
calls the exact Python function the reference implementation uses.
"""

from __future__ import annotations

import contextlib

import duckdb
import pyarrow as pa
from dbt.adapters.duckdb.plugins import BasePlugin
from duckdb.sqltypes import DOUBLE, VARCHAR

from standardization.similarity import double_metaphone, jaro_winkler


def _jaro_winkler_arrow(left: pa.Array, right: pa.Array) -> pa.Array:
    return pa.array(
        [jaro_winkler(a, b) for a, b in zip(left.to_pylist(), right.to_pylist(), strict=True)],
        pa.float64(),
    )


def _double_metaphone_arrow(names: pa.Array) -> pa.Array:
    return pa.array(
        [None if n is None else list(double_metaphone(n)) for n in names.to_pylist()],
        pa.list_(pa.string()),
    )


def register(connection: duckdb.DuckDBPyConnection) -> None:
    """Create (or replace) the UDFs on a DuckDB connection."""
    for name in ("pmp_jaro_winkler", "pmp_double_metaphone"):
        with contextlib.suppress(Exception):  # not registered yet
            connection.remove_function(name)
    connection.create_function(
        "pmp_jaro_winkler", _jaro_winkler_arrow, [VARCHAR, VARCHAR], DOUBLE, type="arrow"
    )
    connection.create_function(
        "pmp_double_metaphone",
        _double_metaphone_arrow,
        [VARCHAR],
        duckdb.list_type(VARCHAR),
        type="arrow",
    )


class Plugin(BasePlugin):
    """dbt-duckdb plugin hook."""

    def configure_connection(self, conn: duckdb.DuckDBPyConnection) -> None:
        register(conn)
