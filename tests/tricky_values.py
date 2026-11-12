"""A hand-made load of tricky raw values, shared by the dbt and Spark tests.

Every awkward input the standardization rules handle appears here: accents and
non-Latin letters, apostrophes and hyphens, particles and suffixes,
placeholders, SSNs mangled by spreadsheets, and dates in every accepted and
rejected format. Hand-built variants of one person make every tier and blocker
occur at least once.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from pipeline import paths

TRICKY_FIRST = [
    "José",
    "José",
    "SØREN",
    "Mary-Ann",
    "MaryAnn",
    "Mary Ann",
    "D.",
    "David",
    "Dave",
    "Chris",
    "Christina",
    "Baby Boy",
    "FNU",
    "Zoë Ἀλέξανδρος",
    "'Neil",
    "Anne--Marie",
    "Catherine",
    "Katherine",
    "Catherime",
    "MAXIMI",
    "Maximilian",
    "John III",
    "",
    None,
]
TRICKY_LAST = [
    "Müller",
    "Strauß",
    "O’Brien",
    "OBrien",
    "De la Cruz",
    "Delacruz",
    "Cruz",
    "García López",
    "Lopez",
    "Smith, Jr.",
    "Smith Sr",
    "SMITH JUNIOR",
    "Smyth",
    "-Smith-",
    "LNU",
    "Li (Lee)",
    "Van der Berg",
    "Al Hassan",
    None,
]
TRICKY_SSN = [
    "234-56-7801",
    "234 56 7801",
    "234567801",
    "234-56-7811",
    "234-56-7810",
    "12345678",
    "12345678.0",
    "1.23456780E+07",
    "2.345678E+08",
    "2.345678015E+08",
    "N/A",
    "234-56-780",
    "000-00-0000",
    "123-45-6789",
    "912-70-1234",
    "666-12-3456",
    "234-00-5678",
    "234-56-0000",
    " 234-56-7801\t",
    "",
    None,
]
TRICKY_DOB = [
    "1980-05-06",
    "1980-5-6",
    "1980-05-06 00:00:00",
    "05/06/1980",
    "19800506",
    "06/05/1980",
    "1980-05-08",
    "1981-05-06",
    "1984-02-29",
    "1984-02-28",
    "1984-03-01",
    "1900-01-01",
    "1980-01-01",
    "05/06/80",
    "05/06/15",
    "1980",
    "1980-02-30",
    "0000-01-01",
    "2025-06-02",
    "May 6 1980",
    "2025-03-01",
    "1990-03-01",
    "",
    None,
]
TRICKY_GENDER = ["F", "M", "f", " Female ", "X", "U", "?", "Q", "", None]
TRICKY_TODAY = date(2025, 6, 1)


def tricky_rows() -> list[dict]:
    """Deterministic mix of tricky values, dense enough that many pairs share keys."""
    columns = [TRICKY_FIRST, TRICKY_LAST, TRICKY_SSN, TRICKY_DOB, TRICKY_GENDER]
    rows = []
    for i in range(600):
        first, last, ssn, dob, gender = (
            col[(i * step) % len(col)] for col, step in zip(columns, (1, 3, 7, 5, 2), strict=True)
        )
        rows.append(
            {
                "source_system": ["EHR_A", "CLAIMS_B", "LAB_C"][i % 3],
                "source_record_id": f"T{i:05d}",
                "record_life": 1 + i % 2,
                "first_name": first,
                "last_name": last,
                "dob": dob,
                "ssn": ssn,
                "gender": gender,
            }
        )
    # Hand-built variants of one person so every tier and blocker occurs.
    base = {
        "first_name": "Catherine",
        "last_name": "Miller",
        "dob": "1980-05-06",
        "ssn": "345-67-8901",
        "gender": "F",
    }
    variants = [
        {},
        {"ssn": "345-67-8911"},
        {"ssn": "345-67-8910"},
        {"dob": "1980-05-08"},
        {"dob": "1980-06-05"},
        {"first_name": "Katherine", "ssn": "345-67-8911"},
        {"first_name": "Miller", "last_name": "Catherine"},
        {"last_name": "Okafor"},
        {"first_name": "Emma", "ssn": "456-78-9012"},
        {"first_name": "Derek", "last_name": "Novak", "dob": "1972-09-14"},
        {"last_name": "Miller Jr"},
        {"last_name": "Miller Sr"},
        {"first_name": "Baby Girl", "dob": "2025-03-01"},
        {"ssn": ""},
        {"dob": "1980-01-01"},
    ]
    for i, change in enumerate(variants):
        rows.append(
            {
                "source_system": "EHR_A",
                "source_record_id": f"V{i:05d}",
                "record_life": 1,
                **base,
                **change,
            }
        )
    return rows


RAW_SCHEMA = pa.schema(
    [
        ("source_system", pa.string()),
        ("source_record_id", pa.string()),
        ("record_life", pa.int64()),
        ("first_name", pa.string()),
        ("last_name", pa.string()),
        ("dob", pa.string()),
        ("ssn", pa.string()),
        ("gender", pa.string()),
    ]
)


def write_tricky_load(data_dir: Path) -> list[dict]:
    """Write the tricky rows as ingested load 0 of ``data_dir``; returns the rows."""
    rows = tricky_rows()
    ingested = paths.ingested_dir(0, data_dir)
    ingested.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(rows, RAW_SCHEMA), ingested / "current_records.parquet")
    manifest = paths.load_dir(0, data_dir)
    manifest.mkdir(parents=True)
    (manifest / "manifest.json").write_text(json.dumps({"load_date": TRICKY_TODAY.isoformat()}))
    return rows
