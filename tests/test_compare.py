"""Unit tests for similarity primitives and every comparison level (DESIGN.md section 7)."""

from __future__ import annotations

from datetime import date

import pytest
from matching_helpers import pair, rules

from standardization.compare import (
    compare_dob,
    compare_forms,
    compare_name,
    compare_ssn,
    years_between,
)
from standardization.similarity import double_metaphone, jaro_winkler, phonetic_match

# --------------------------------------------------------------------------- #
# Similarity primitives (shared by dbt and PySpark, D-009)
# --------------------------------------------------------------------------- #


def test_jaro_winkler_known_values():
    assert jaro_winkler("MARTHA", "MARHTA") == pytest.approx(0.9611, abs=1e-4)
    assert jaro_winkler("DWAYNE", "DUANE") == pytest.approx(0.84, abs=1e-4)
    assert jaro_winkler("SMITH", "SMITH") == 1.0
    assert jaro_winkler(None, "SMITH") is None


def test_double_metaphone_codes():
    assert double_metaphone("SMITH") == ("SM0", "XMT")
    assert double_metaphone("") == () and double_metaphone(None) == ()
    assert phonetic_match("SMITH", "SMYTH")
    assert phonetic_match("CATHERINE", "KATHERINE")
    assert not phonetic_match("SMITH", "JONES")
    assert not phonetic_match(None, "SMITH")


# --------------------------------------------------------------------------- #
# SSN and DOB levels
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("a", "b", "level"),
    [
        ("234567801", "234567801", "EXACT"),
        ("234567801", "234567811", "NEAR"),  # one substitution
        ("234567801", "234567810", "NEAR"),  # adjacent transposition
        ("234567801", "324567801", "NEAR"),
        ("234567801", "234567180", "DIFFERENT"),  # transposition two apart
        ("234567801", "234567899", "DIFFERENT"),  # two substitutions
        ("234567801", "432567801", "DIFFERENT"),
        (None, "234567801", "MISSING"),
        (None, None, "MISSING"),
    ],
)
def test_ssn_levels(a, b, level):
    assert compare_ssn(a, b) == level
    assert compare_ssn(b, a) == level


@pytest.mark.parametrize(
    ("a", "b", "level"),
    [
        (date(1980, 5, 6), date(1980, 5, 6), "EXACT"),
        (date(1980, 3, 4), date(1980, 4, 3), "NEAR"),  # month/day swap
        (date(1980, 5, 6), date(1980, 5, 8), "NEAR"),  # one digit
        (date(1980, 5, 6), date(1990, 5, 6), "NEAR"),  # one digit in the year
        (date(1980, 5, 6), date(1981, 5, 6), "NEAR"),  # year +1
        (date(1989, 12, 31), date(1990, 12, 31), "NEAR"),  # year +1, many digits differ
        (date(1984, 2, 29), date(1984, 2, 28), "NEAR"),  # leap day
        (date(1984, 2, 29), date(1984, 3, 1), "NEAR"),
        (date(1984, 2, 29), date(1985, 3, 1), "DIFFERENT"),
        (date(1980, 5, 6), date(1982, 5, 6), "NEAR"),  # one digit: 1980 -> 1982
        (date(1980, 5, 6), date(1983, 6, 6), "DIFFERENT"),
        (date(1980, 5, 6), date(1980, 7, 9), "DIFFERENT"),
        (None, date(1980, 5, 6), "MISSING"),
    ],
)
def test_dob_levels(a, b, level):
    assert compare_dob(a, b) == level
    assert compare_dob(b, a) == level


@pytest.mark.parametrize(
    ("a", "b", "years"),
    [
        (date(1980, 5, 6), date(1992, 5, 6), 12),
        (date(1980, 5, 6), date(1992, 5, 5), 11),
        (date(1980, 5, 6), date(1980, 5, 6), 0),
        (date(2000, 2, 29), date(2012, 2, 28), 11),
        (None, date(1980, 5, 6), None),
    ],
)
def test_years_between(a, b, years):
    assert years_between(a, b) == years == years_between(b, a)


# --------------------------------------------------------------------------- #
# Name levels
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("a", "b", "level", "method"),
    [
        ("DAVID", "DAVID", "STRONG", "EXACT"),
        ("DAVE", "DAVID", "STRONG", "VARIANT"),
        ("BOB", "ROB", "STRONG", "VARIANT"),  # two variants of ROBERT (D-031)
        ("MOHAMED", "MUHAMMED", "STRONG", "VARIANT"),
        ("SMITH", "SMYTH", "STRONG", "PHONETIC"),
        ("CATHERIME", "CATHERINE", "STRONG", "FUZZY"),
        ("MAXIMI", "MAXIMILIAN", "STRONG", "PREFIX"),  # 6 chars: prefix rule
        ("MAXIM", "MAXIMILIAN", "WEAK", "FUZZY_WEAK"),  # 5 chars: no prefix rule
        ("D", "DAVID", "WEAK", "INITIAL"),
        ("D", "D", "WEAK", "INITIAL"),  # two initials are never strong
        ("D", "MARK", "NONE", "NONE"),
        ("EMMA", "OLIVIA", "NONE", "NONE"),
    ],
)
def test_form_levels(a, b, level, method):
    assert compare_forms(a, b, rules().names, genders_conflict=False) == (level, method)
    assert compare_forms(b, a, rules().names, genders_conflict=False) == (level, method)


def test_thresholds_come_from_the_profile():
    # No phonetic or variant link, so only Jaro-Winkler decides.
    # JW(JONES, JOHNSON) = 0.832; JW(BAKER, BARKER) = 0.902.
    assert compare_forms("JONES", "JOHNSON", rules("conservative").names, False)[0] == "NONE"
    assert compare_forms("JONES", "JOHNSON", rules("aggressive").names, False)[0] == "WEAK"
    assert compare_forms("BAKER", "BARKER", rules("conservative").names, False)[0] == "WEAK"
    assert compare_forms("BAKER", "BARKER", rules("aggressive").names, False)[0] == "STRONG"


def test_gender_ambiguous_nickname_with_conflicting_genders_is_weak():
    names = rules().names
    assert compare_forms("CHRIS", "CHRISTINA", names, genders_conflict=False) == (
        "STRONG",
        "VARIANT",
    )
    assert compare_forms("CHRIS", "CHRISTINA", names, genders_conflict=True) == (
        "WEAK",
        "VARIANT_GENDER_CONFLICT",
    )
    # Not gender-ambiguous: the conflict does not matter.
    assert compare_forms("DAVE", "DAVID", names, genders_conflict=True) == ("STRONG", "VARIANT")


def test_gender_conflict_only_counts_between_non_null_genders():
    assert (
        pair(
            {"first_name": "Chris"}, {"first_name": "Christina", "gender": "M"}
        ).comparison.first_level
        == "WEAK"
    )
    assert (
        pair(
            {"first_name": "Chris", "gender": "U"}, {"first_name": "Christina", "gender": "M"}
        ).comparison.first_level
        == "STRONG"
    )


def test_best_level_over_all_forms_and_tokens():
    names = rules().names
    assert compare_name(("ANN", "MARY", "MARY ANN"), ("MARY",), names, False) == ("STRONG", "EXACT")
    assert compare_name((), ("MARY",), names, False) == ("NONE", "MISSING")


def test_swapped_names_are_strong_and_flagged():
    c = pair({}, {"first_name": "Miller", "last_name": "Catherine"}).comparison
    assert (c.first_level, c.last_level, c.names_swapped) == ("STRONG", "STRONG", True)
    c = pair().comparison
    assert not c.names_swapped


def test_swap_needs_both_cross_comparisons_strong():
    c = pair({}, {"first_name": "Miller", "last_name": "Jones"}).comparison
    assert not c.names_swapped and c.first_level == "NONE"


def test_gender_is_never_positive_evidence():
    """Agreeing, disagreeing or missing gender leaves every level and decision unchanged."""
    baseline = pair({"first_name": "Emma"}, {"first_name": "Emily", "ssn": "234-56-7811"})
    for gender in ("F", "M", "X", ""):
        other = pair(
            {"first_name": "Emma"}, {"first_name": "Emily", "ssn": "234-56-7811", "gender": gender}
        )
        assert other == baseline
