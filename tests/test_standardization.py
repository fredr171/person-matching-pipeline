"""Unit tests for every standardization function (DESIGN.md section 5)."""

from __future__ import annotations

from datetime import date

import pytest
from matching_helpers import TODAY, record, tables

from standardization.standardize import (
    clean_name,
    name_forms,
    split_suffix,
    standardize_dob,
    standardize_gender,
    standardize_ssn,
    strip_particle,
)

# --------------------------------------------------------------------------- #
# Names
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("José", "JOSE"),
        ("José", "JOSE"),  # decomposed accent
        ("SØREN", "SOREN"),  # no Unicode decomposition: table-driven
        ("Müller", "MULLER"),
        ("Strauß", "STRAUSS"),
        ("  mary   ann ", "MARY ANN"),
        ("O’Brien", "O'BRIEN"),  # typographic apostrophe
        ("Smith, Jr.", "SMITH JR"),
        ("D.", "D"),
        ("Mary-Ann", "MARY-ANN"),
        ("-Smith-", "SMITH"),  # hyphen not inside a name
        ("'Neil", "NEIL"),
        ("Anne--Marie", "ANNE MARIE"),
        ("Li (Lee)", "LI LEE"),
        ("Zoë Ἀλέξανδρος", "ZOE"),  # unmapped script is removed
        ("", None),
        ("  .,  ", None),
        (None, None),
    ],
)
def test_clean_name(raw, expected):
    assert clean_name(raw, tables().letters) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("SMITH JR", ("SMITH", "JR")),
        ("SMITH JUNIOR", ("SMITH", "JR")),
        ("SMITH 3RD", ("SMITH", "III")),
        ("SMITH II", ("SMITH", "II")),
        ("JUNIOR", ("JUNIOR", None)),  # a suffix needs another token to remain
        ("JR SMITH", ("JR SMITH", None)),  # only trailing tokens
        ("SMITH", ("SMITH", None)),
        (None, (None, None)),
    ],
)
def test_split_suffix(text, expected):
    assert split_suffix(text, tables().suffixes) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("DE LA CRUZ", "CRUZ"),  # longest particle first
        ("DE CRUZ", "CRUZ"),
        ("VAN DER BERG", "BERG"),
        ("O'BRIEN", "BRIEN"),
        ("AL HASSAN", "HASSAN"),
        ("DELACRUZ", None),  # concatenated: handled by the space-removed form
        ("DEAN", None),  # a particle must be a separate word
        ("DE", None),
        ("O'", None),
    ],
)
def test_strip_particle(text, expected):
    assert strip_particle(text, tables().particles) == expected


def test_name_forms_cover_every_comparison_form():
    assert name_forms("DE LA CRUZ", tables()) == ("CRUZ", "DE LA CRUZ", "DELACRUZ")
    assert name_forms("MARY-ANN", tables()) == ("ANN", "MARY", "MARY ANN", "MARY-ANN", "MARYANN")
    assert name_forms("O'BRIEN", tables()) == ("BRIEN", "O'BRIEN", "OBRIEN")
    assert name_forms("GARCIA LOPEZ", tables()) == (
        "GARCIA",
        "GARCIA LOPEZ",
        "GARCIALOPEZ",
        "LOPEZ",
    )
    assert name_forms("SMITH", tables()) == ("SMITH",)
    assert name_forms(None, tables()) == ()


def test_name_forms_skip_particles_and_initials_as_tokens():
    forms = name_forms("JUAN DE SOUZA", tables())
    assert "DE" not in forms and "SOUZA" in forms and "JUAN" in forms
    assert "J" not in name_forms("J ROBERT", tables())


@pytest.mark.parametrize(
    ("first", "last"),
    [
        ("Baby Boy", "Smith"),
        ("Infant", "Smith"),
        ("Unknown", "Smith"),
        ("FNU", "Smith"),
        ("Test", "User"),
        ("Ana", "LNU"),
        ("Ana", "asdf"),
        ("Baby Girl", "Smith"),
    ],
)
def test_placeholder_names_become_null(first, last):
    r = record(first_name=first, last_name=last)
    placeholders = tables().name_placeholders
    if first.upper() in placeholders:
        assert r.first_name is None and r.first_name_is_placeholder and r.first_name_forms == ()
    if last.upper() in placeholders:
        assert r.last_name is None and r.last_name_is_placeholder
    assert r.has_placeholder_name


def test_suffix_comes_from_last_name_or_first_name():
    assert record(last_name="Smith, Jr").suffix == "JR"
    assert record(last_name="Smith", first_name="John III").suffix == "III"
    r = record(last_name="Smith Sr.")
    assert r.last_name == "SMITH" and r.last_name_raw == "Smith Sr."


# --------------------------------------------------------------------------- #
# SSN
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("234-56-7801", "234567801"),
        ("234 56 7801", "234567801"),
        ("234567801", "234567801"),
        (" 234-56-7801 ", "234567801"),
        ("12345678", "012345678"),  # leading zero lost
        ("1234567", "001234567"),
        ("12345678.0", "012345678"),
        ("234567801.00", "234567801"),
        ("2.34567801E+08", "234567801"),
        ("1.23456780E+07", "012345678"),
        ("2.34567801e8", "234567801"),
    ],
)
def test_ssn_is_recovered(raw, expected):
    assert standardize_ssn(raw, tables().ssn_placeholders) == (expected, None)


@pytest.mark.parametrize(
    ("raw", "reason"),
    [
        (None, "MISSING"),
        ("", "MISSING"),
        ("   ", "MISSING"),
        ("2.345678E+08", "UNRECOVERABLE"),  # digits rounded away
        ("2.345678015E+08", "UNRECOVERABLE"),  # not an integer
        ("N/A", "UNPARSEABLE"),
        ("234-56-780", "INVALID_LENGTH"),  # formatted values are not padded
        ("2345678011", "INVALID_LENGTH"),
        ("000-00-0000", "PLACEHOLDER"),
        ("123-45-6789", "PLACEHOLDER"),
        ("999-99-9999", "PLACEHOLDER"),
        ("111111111", "PLACEHOLDER"),
        ("078-05-1120", "PLACEHOLDER"),
        ("912-70-1234", "ITIN_OR_TEMPORARY"),
        ("000-12-3456", "INVALID_AREA"),
        ("666-12-3456", "INVALID_AREA"),
        ("234-00-5678", "INVALID_GROUP"),
        ("234-56-0000", "INVALID_SERIAL"),
    ],
)
def test_ssn_is_nulled_with_a_reason(raw, reason):
    assert standardize_ssn(raw, tables().ssn_placeholders) == (None, reason)


# --------------------------------------------------------------------------- #
# DOB
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1980-05-06", date(1980, 5, 6)),
        ("1980-5-6", date(1980, 5, 6)),
        ("1980-05-06 00:00:00", date(1980, 5, 6)),
        ("1980-05-06T00:00:00", date(1980, 5, 6)),
        ("05/06/1980", date(1980, 5, 6)),
        ("5/6/1980", date(1980, 5, 6)),
        ("19800506", date(1980, 5, 6)),
        ("05/06/15", date(2015, 5, 6)),  # two-digit year read as 20YY
        ("2024-02-29", date(2024, 2, 29)),
        ("2025-06-01", TODAY),  # born on the load date is not future
    ],
)
def test_dob_is_parsed(raw, expected):
    assert standardize_dob(raw, tables().dob_placeholders, TODAY) == (expected, None)


@pytest.mark.parametrize(
    ("raw", "reason"),
    [
        (None, "MISSING"),
        ("", "MISSING"),
        ("1980", "YEAR_ONLY"),
        ("May 6 1980", "UNPARSEABLE"),
        ("06.05.1980", "UNPARSEABLE"),
        ("1980-02-30", "INVALID_DATE"),
        ("13/01/1980", "INVALID_DATE"),
        ("2023-02-29", "INVALID_DATE"),
        ("1900-01-01", "PLACEHOLDER"),
        ("01/01/1901", "PLACEHOLDER"),
        ("05/06/80", "FUTURE"),  # DOB_TWO_DIGIT_YEAR: 2080
        ("2025-06-02", "FUTURE"),
    ],
)
def test_dob_is_nulled_with_a_reason(raw, reason):
    assert standardize_dob(raw, tables().dob_placeholders, TODAY) == (None, reason)


def test_jan_first_is_kept_and_flagged():
    r = record(dob="1980-01-01")
    assert r.dob == date(1980, 1, 1) and r.dob_is_jan1
    assert not record().dob_is_jan1
    assert not record(dob="1980").dob_is_jan1


# --------------------------------------------------------------------------- #
# Gender
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("M", "M"),
        ("f", "F"),
        (" Female ", "F"),
        ("MALE", "M"),
        ("X", "X"),
        ("U", None),
        ("?", None),
        ("Q", None),
        ("1", None),
        ("", None),
        (None, None),
    ],
)
def test_gender(raw, expected):
    assert standardize_gender(raw, tables().genders) == expected


def test_raw_values_are_kept():
    r = record(first_name="José", ssn="12345678.0", dob="05/06/1980", gender="female")
    assert (r.first_name_raw, r.ssn_raw, r.dob_raw, r.gender_raw) == (
        "José",
        "12345678.0",
        "05/06/1980",
        "female",
    )
    assert (r.first_name, r.ssn, r.dob, r.gender) == ("JOSE", "012345678", date(1980, 5, 6), "F")


# --------------------------------------------------------------------------- #
# Generated data
# --------------------------------------------------------------------------- #

# Edge cases whose distortion stays on the record while it is tagged, and the
# standardized result each must produce. (Tags are cumulative over a record's
# life, so cases a later update can undo, such as placeholders, are left out.)
PERSISTENT_CASES = {
    "SSN_NUMERIC_CORRUPT": lambda r: r.ssn is not None,
    "SSN_PLACEHOLDER": lambda r: r.ssn_null_reason == "PLACEHOLDER",
    "SSN_TEMP_ITIN": lambda r: r.ssn_null_reason == "ITIN_OR_TEMPORARY",
    "DOB_TWO_DIGIT_YEAR": lambda r: r.dob_null_reason == "FUTURE",
    "DOB_YEAR_ONLY": lambda r: r.dob_null_reason == "YEAR_ONLY",
}


def test_generated_records_standardize_as_their_edge_cases_expect(ingested_boosted, boosted_truth):
    import json

    import pyarrow.parquet as pq

    from pipeline import paths
    from standardization.standardize import standardize_record

    data_dir, metrics = ingested_boosted
    last = len(metrics) - 1
    manifest = json.loads((paths.load_dir(last, data_dir) / "manifest.json").read_text())
    today = date.fromisoformat(manifest["load_date"])
    tags = {}
    for load_id in sorted(boosted_truth):
        for row in boosted_truth[load_id]:
            tags[(row["source_system"], row["source_record_id"])] = set(row["edge_case_tags"])
    current = pq.read_table(paths.ingested_dir(last, data_dir) / "current_records.parquet")
    checked = dict.fromkeys(PERSISTENT_CASES, 0)
    for row in current.to_pylist():
        r = standardize_record(row, tables(), today)
        assert r.dob is None or r.dob <= today
        for code, expected in PERSISTENT_CASES.items():
            if code in tags[r.key]:
                assert expected(r), (code, row)
                checked[code] += 1
    assert all(checked.values()), checked
