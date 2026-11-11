"""Builders for hand-made records and pair decisions used by the matching tests."""

from __future__ import annotations

from datetime import date
from functools import cache

from pipeline import config, reference
from standardization.rules import PairDecision, PairRules, decide
from standardization.standardize import StandardizationTables, StandardRecord, standardize_record

TODAY = date(2025, 6, 1)

# One ordinary person; scenarios change one or two fields of a copy.
BASE = {
    "source_system": "EHR_A",
    "first_name": "Catherine",
    "last_name": "Miller",
    "dob": "1980-05-06",
    "ssn": "234-56-7801",
    "gender": "F",
}

_ids = iter(range(1, 10**9))


@cache
def tables() -> StandardizationTables:
    return StandardizationTables.from_reference(reference.load_reference_data())


@cache
def rules(profile: str = "conservative") -> PairRules:
    return PairRules.build(reference.load_reference_data(), config.load_matching_config(), profile)


def record(**changes) -> StandardRecord:
    """A standardized copy of BASE with some raw fields changed."""
    row = {**BASE, "source_record_id": f"R{next(_ids):06d}", **changes}
    return standardize_record(row, tables(), TODAY)


def pair(changes_a: dict | None = None, changes_b: dict | None = None, profile="conservative"):
    """Decision for BASE+changes_a against BASE+changes_b."""
    return decide(record(**(changes_a or {})), record(**(changes_b or {})), rules(profile), TODAY)


def decide_records(a: StandardRecord, b: StandardRecord, profile="conservative") -> PairDecision:
    return decide(a, b, rules(profile), TODAY)
