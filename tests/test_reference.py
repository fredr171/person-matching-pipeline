"""Reference data parses cleanly and covers every value DESIGN.md names."""

from __future__ import annotations

import re
from datetime import date

import pytest

from pipeline import reference

# Standardized form (DESIGN.md section 5): uppercase ASCII, digits, single spaces,
# hyphens and apostrophes. Reference values must already be in this form.
STANDARDIZED = re.compile(r"^[A-Z0-9'\-]+( [A-Z0-9'\-]+)*$")


def test_all_name_values_are_standardized(reference_data):
    values = [v.canonical for v in reference_data.name_variants]
    values += [v.variant for v in reference_data.name_variants]
    values += list(reference_data.placeholder_values("name"))
    values += list(reference_data.particles)
    values += list(reference_data.suffixes) + list(reference_data.suffixes.values())
    bad = sorted({v for v in values if not STANDARDIZED.match(v)})
    assert bad == []


@pytest.mark.parametrize(
    ("variant", "canonical"),
    [
        ("DAVE", "DAVID"),
        ("BOB", "ROBERT"),
        ("PEGGY", "MARGARET"),
        ("MOHAMMED", "MUHAMMAD"),
        ("MOHAMED", "MUHAMMAD"),
    ],
)
def test_design_examples_are_in_name_variants(reference_data, variant, canonical):
    """Examples named in DESIGN.md section 3.7 (NAME_NICKNAME, NAME_TRANSLITERATION)."""
    pairs = {(v.variant, v.canonical) for v in reference_data.name_variants}
    assert (variant, canonical) in pairs


def test_gender_ambiguous_nicknames_are_derived(reference_data):
    """Section 7 names Chris, Alex, Sam, Pat and Jo as gender-ambiguous (D-015)."""
    ambiguous = reference_data.gender_ambiguous_variants()
    assert {"CHRIS", "ALEX", "SAM", "PAT", "JO"} <= ambiguous
    assert "BOB" not in ambiguous


def test_design_name_placeholders_are_present(reference_data):
    """Section 3.7 NAME_PLACEHOLDER examples."""
    names = reference_data.placeholder_values("name")
    assert {"BABY BOY", "INFANT", "UNKNOWN", "FNU", "LNU", "TEST USER", "ASDF"} <= names


def test_strings_that_look_like_null_are_kept_verbatim(reference_data):
    """NA, NULL and NONE are placeholder *values* and must not be read as missing."""
    assert {"NA", "NULL", "NONE"} <= reference_data.placeholder_values("name")


def test_ssn_placeholders_are_nine_digits_and_cover_design(reference_data):
    ssns = reference_data.placeholder_values("ssn")
    assert all(re.fullmatch(r"\d{9}", s) for s in ssns)
    assert {"000000000", "123456789", "999999999"} <= ssns
    # Leading zeros survive loading (a numeric parse would drop them).
    assert "078051120" in ssns


def test_dob_placeholders_are_valid_iso_dates(reference_data):
    dobs = reference_data.placeholder_values("dob")
    assert "1900-01-01" in dobs
    for value in dobs:
        date.fromisoformat(value)


def test_particles_cover_design_list(reference_data):
    """Section 5 particle list."""
    required = {
        "DE",
        "DE LA",
        "DEL",
        "VAN",
        "VAN DER",
        "VON",
        "AL",
        "EL",
        "DA",
        "DI",
        "LE",
        "LA",
        "O'",
    }
    assert required <= set(reference_data.particles)


def test_particles_longest_first_puts_multiword_before_prefix(reference_data):
    ordered = reference_data.particles_longest_first()
    assert ordered.index("DE LA") < ordered.index("DE")
    assert ordered.index("VAN DER") < ordered.index("VAN")


def test_suffixes_cover_design_list(reference_data):
    """Section 5: JR, SR, II, III, IV in all common formats."""
    assert set(reference_data.suffixes.values()) == {"JR", "SR", "II", "III", "IV"}
    assert reference_data.suffixes["JUNIOR"] == "JR"
    assert reference_data.suffixes["3RD"] == "III"


def test_wrong_columns_are_rejected(tmp_path):
    bad = tmp_path / "particles.csv"
    bad.write_text("name\nDE\n", encoding="utf-8")
    with pytest.raises(reference.ReferenceDataError, match="expected columns"):
        reference.load_particles(bad)


def test_duplicate_rows_are_rejected(tmp_path):
    bad = tmp_path / "suffixes.csv"
    bad.write_text("form,canonical\nJR,JR\nJR,JR\n", encoding="utf-8")
    with pytest.raises(reference.ReferenceDataError, match="duplicate"):
        reference.load_suffixes(bad)


def test_self_mapping_variant_is_rejected(tmp_path):
    bad = tmp_path / "nicknames.csv"
    bad.write_text(
        "canonical,canonical_gender,variant,relation\nANNA,F,ANNA,nickname\n", encoding="utf-8"
    )
    with pytest.raises(reference.ReferenceDataError, match="maps to itself"):
        reference.load_name_variants(bad)
