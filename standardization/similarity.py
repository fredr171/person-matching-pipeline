"""Shared string-similarity primitives (DESIGN.md section 10, D-009).

DuckDB has Jaro-Winkler but Spark does not, and neither has Double Metaphone.
Both are implemented once here and registered as a DuckDB UDF (dbt) and as a
pandas UDF (PySpark), so the two implementations score names identically.

Both functions accept ``None`` and return a null-safe result, because SQL
engines call UDFs on null inputs.
"""

from __future__ import annotations

from metaphone import doublemetaphone
from rapidfuzz.distance import JaroWinkler


def jaro_winkler(a: str | None, b: str | None) -> float | None:
    """Jaro-Winkler similarity in [0, 1] (prefix weight 0.1); None if either is null."""
    if a is None or b is None:
        return None
    return JaroWinkler.similarity(a, b, prefix_weight=0.1)


def double_metaphone(name: str | None) -> tuple[str, ...]:
    """The non-empty Double Metaphone codes of ``name`` (primary first, no duplicates)."""
    if not name:
        return ()
    codes = []
    for code in doublemetaphone(name):
        if code and code not in codes:
            codes.append(code)
    return tuple(codes)


def phonetic_match(a: str | None, b: str | None) -> bool:
    """True when the two names share any Double Metaphone code."""
    return bool(set(double_metaphone(a)) & set(double_metaphone(b)))
