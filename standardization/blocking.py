"""Blocking: which record pairs are compared at all (DESIGN.md section 6).

Records sharing a value of any blocking key become candidate pairs. A record
without the fields a key needs gets no value for it. Blocks larger than
``max_block_size`` are skipped and reported, because a spike usually means a
new placeholder value. Pairs are deduplicated across keys and written with
the smaller record key first.

Key values (D-040):

=======================================  ===========================================
Key                                      Value
=======================================  ===========================================
ssn                                      the 9-digit SSN
dob                                      the DOB as YYYY-MM-DD
ssn_sorted_digits_birth_year             SSN digits sorted ascending, ``|``, birth year
ssn_first5_birth_year                    first 5 SSN digits, ``|``, birth year
last_phonetic_first_initial_birth_year   primary Double Metaphone code of the last
                                         name with spaces, hyphens and apostrophes
                                         removed, ``|``, first letter of the first
                                         name, ``|``, birth year
=======================================  ===========================================
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass

from standardization.similarity import double_metaphone
from standardization.standardize import StandardRecord

BLOCKING_KEYS = (
    "ssn",
    "dob",
    "ssn_sorted_digits_birth_year",
    "ssn_first5_birth_year",
    "last_phonetic_first_initial_birth_year",
)


def record_key(record: StandardRecord) -> str:
    """The identifier of a live source record across all engines: ``SOURCE|ID``."""
    return f"{record.source_system}|{record.source_record_id}"


def last_name_key(last_name: str) -> str:
    return last_name.replace(" ", "").replace("-", "").replace("'", "")


def key_values(record: StandardRecord) -> dict[str, str]:
    """The blocking key values a record has (keys it lacks the fields for are absent)."""
    values = {}
    ssn, dob = record.ssn, record.dob
    year = f"{dob.year:04d}" if dob else None
    if ssn:
        values["ssn"] = ssn
    if dob:
        values["dob"] = dob.isoformat()
    if ssn and year:
        values["ssn_sorted_digits_birth_year"] = "".join(sorted(ssn)) + "|" + year
        values["ssn_first5_birth_year"] = ssn[:5] + "|" + year
    if record.last_name and record.first_name and year:
        codes = double_metaphone(last_name_key(record.last_name))
        if codes:
            values["last_phonetic_first_initial_birth_year"] = (
                f"{codes[0]}|{record.first_name[0]}|{year}"
            )
    return values


@dataclass(frozen=True)
class SkippedBlock:
    key_name: str
    key_value: str
    block_size: int


@dataclass(frozen=True)
class Candidates:
    """Candidate pairs (each with the keys that produced it) and skipped blocks."""

    pairs: dict[tuple[str, str], tuple[str, ...]]
    skipped_blocks: tuple[SkippedBlock, ...]


def candidate_pairs(
    records: Iterable[StandardRecord], max_block_size: int, keys: tuple[str, ...] = BLOCKING_KEYS
) -> Candidates:
    blocks: dict[tuple[str, str], list[str]] = defaultdict(list)
    for record in records:
        for name, value in key_values(record).items():
            if name in keys:
                blocks[(name, value)].append(record_key(record))

    pairs: dict[tuple[str, str], set[str]] = defaultdict(set)
    skipped = []
    for (name, value), members in sorted(blocks.items()):
        if len(members) > max_block_size:
            skipped.append(SkippedBlock(name, value, len(members)))
            continue
        members = sorted(set(members))
        for i, a in enumerate(members):
            for b in members[i + 1 :]:
                pairs[(a, b)].add(name)
    return Candidates(
        pairs={pair: tuple(sorted(names)) for pair, names in sorted(pairs.items())},
        skipped_blocks=tuple(skipped),
    )
