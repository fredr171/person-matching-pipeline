"""Unit tests for generator building blocks: identifiers, rendering, particles."""

from __future__ import annotations

import unicodedata
from datetime import date
from decimal import Decimal

import pytest
from generator_helpers import make_context

from generator import identifiers as ids
from generator.model import Identity, IdentityChange, Override, Person, Record
from generator.record_cases import split_particle
from generator.render import SourceState, render
from generator.rng import stream

# --------------------------------------------------------------------------- #
# SSNs
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("ssn", "valid"),
    [
        ("123456780", True),
        ("000123456", False),  # area 000
        ("666123456", False),  # area 666
        ("912345678", False),  # area 9xx (ITIN range)
        ("123004567", False),  # group 00
        ("123450000", False),  # serial 0000
        ("12345678", False),
    ],
)
def test_ssn_validity_rules(ssn, valid):
    assert ids.is_valid_ssn(ssn) is valid


def test_ssn_substitution_changes_exactly_one_digit():
    rng = stream(1, "t")
    for _ in range(200):
        result = ids.ssn_substitute_digit(rng, "123456789")
        assert sum(a != b for a, b in zip(result, "123456789", strict=True)) == 1


def test_ssn_transposition_swaps_adjacent_digits():
    rng = stream(2, "t")
    for _ in range(200):
        result = ids.ssn_transpose_adjacent(rng, "123456789")
        diffs = [i for i in range(9) if result[i] != "123456789"[i]]
        assert len(diffs) == 2 and diffs[1] == diffs[0] + 1
        assert sorted(result) == sorted("123456789")


@pytest.mark.parametrize("kind", ["drop_leading_zeros", "float_suffix", "scientific"])
def test_numeric_corruption_is_recoverable_without_loss(kind):
    ssn = "012345678"
    text = ids.numeric_corruption(ssn, kind)
    assert text != ssn
    recovered = str(int(Decimal(text))).zfill(9)
    assert recovered == ssn


def test_allocator_never_hands_out_duplicates_or_placeholders():
    allocator = ids.SsnAllocator(stream(3, "t"), frozenset({"123456789"}))
    issued = [allocator.new() for _ in range(2000)]
    assert len(set(issued)) == len(issued)
    assert "123456789" not in issued
    assert all(ids.is_valid_ssn(s) for s in issued)


def test_sibling_ssn_is_near_and_unique():
    allocator = ids.SsnAllocator(stream(4, "t"), frozenset())
    base = allocator.new()
    near = allocator.near(base)
    assert near != base and near[:5] == base[:5]


def test_itin_starts_with_nine():
    rng = stream(5, "t")
    assert all(ids.temporary_itin(rng).startswith("9") for _ in range(50))


# --------------------------------------------------------------------------- #
# DOBs
# --------------------------------------------------------------------------- #


def test_month_day_swap_only_when_unambiguous():
    assert ids.dob_month_day_swap(date(1980, 3, 4)) == date(1980, 4, 3)
    assert ids.dob_month_day_swap(date(1980, 3, 3)) is None
    assert ids.dob_month_day_swap(date(1980, 3, 14)) is None


def test_dob_digit_typo_changes_one_digit_and_stays_valid():
    rng = stream(6, "t")
    dob = date(1985, 6, 17)
    for _ in range(200):
        value = ids.dob_digit_typo(rng, dob, date(2025, 1, 1))
        assert value is not None and value < date(2025, 1, 1)
        a, b = dob.strftime("%Y%m%d"), value.strftime("%Y%m%d")
        assert sum(x != y for x, y in zip(a, b, strict=True)) == 1


def test_shift_years_handles_leap_day():
    assert ids.shift_years(date(2000, 2, 29), 1) is None
    assert ids.shift_years(date(2000, 3, 1), -1) == date(1999, 3, 1)


@pytest.mark.parametrize(
    ("style", "expected"),
    [("iso", "1985-06-07"), ("us", "06/07/1985"), ("compact", "19850607")],
)
def test_dob_formats(style, expected):
    assert ids.format_dob(date(1985, 6, 7), style) == expected


def test_add_months_clamps_day():
    assert ids.add_months(date(2025, 1, 31), 1) == date(2025, 2, 28)
    assert ids.add_months(date(2025, 11, 1), 2) == date(2026, 1, 1)


# --------------------------------------------------------------------------- #
# Rendering
# --------------------------------------------------------------------------- #


def _person(**identity) -> Person:
    values = {
        "first": "David",
        "middle": "Lee",
        "last": "Smith",
        "suffix": None,
        "dob": date(1980, 5, 6),
        "ssn": "123456780",
        "gender": "M",
    }
    values.update(identity)
    return Person("T1", "anglo", Identity(**values), record_count=1)


@pytest.fixture
def ctx(generator_config):
    return make_context(generator_config)


def _render(ctx, record, source="EHR_A", load=0, migrated=False):
    state = SourceState.initial(ctx.config.source(source))
    if migrated:
        state.migrate(ctx.config.pipeline_cases.migration)
    return render(record, load, state, ctx.config.pipeline_cases.migration)


def test_render_uses_source_formats(ctx):
    record = Record("EHR_A", "EA1", _person(), 0)
    assert _render(ctx, record) == {
        "first_name": "David",
        "last_name": "Smith",
        "dob": "1980-05-06",
        "ssn": "123-45-6780",
        "gender": "M",
    }
    claims = Record("CLAIMS_B", "CB1", _person(), 0)
    row = _render(ctx, claims, "CLAIMS_B")
    assert (row["first_name"], row["dob"], row["ssn"]) == ("DAVID", "05/06/1980", "123456780")


def test_override_stops_applying_when_identity_changes(ctx):
    """A typo in the maiden name disappears once the source records the new surname."""
    person = _person()
    person.changes.append(IdentityChange(3, "MARRIAGE", {"last": "Jones"}))
    record = Record("EHR_A", "EA1", person, 0)
    record.overrides["last"] = Override("Smtih", "Smith", "NAME_TYPO")
    assert _render(ctx, record, load=2)["last_name"] == "Smtih"
    assert _render(ctx, record, load=3)["last_name"] == "Jones"


def test_override_until_load_models_a_correction(ctx):
    record = Record("EHR_A", "EA1", _person(), 0)
    record.overrides["ssn"] = Override(None, "123456780", "SSN_DELAYED", until_load=4)
    assert _render(ctx, record, load=3)["ssn"] == ""
    assert _render(ctx, record, load=4)["ssn"] == "123-45-6780"


def test_new_records_only_change_keeps_old_value_on_existing_records(ctx):
    person = _person()
    person.changes.append(IdentityChange(5, "SSN_REPLACEMENT", {"ssn": "222334444"}, True))
    old = Record("EHR_A", "EA1", person, 0)
    new = Record("EHR_A", "EA2", person, 6)
    assert _render(ctx, old, load=6)["ssn"] == "123-45-6780"
    assert _render(ctx, new, load=6)["ssn"] == "222-33-4444"


def test_structural_flags(ctx):
    record = Record("EHR_A", "EA1", _person(suffix="JR"), 0)
    record.flags.update(
        middle_in_first=True, concatenated=True, suffix_style=(1, ", "), swapped=True
    )
    row = _render(ctx, record)
    assert row["first_name"] == "Smith, Jr."
    assert row["last_name"] == "Davidlee"


@pytest.mark.parametrize(
    ("style", "first", "last"),
    [("repeat", "Madonna", "Madonna"), ("lnu", "Madonna", "Lnu"), ("fnu", "Fnu", "Madonna")],
)
def test_mononym_styles(ctx, style, first, last):
    person = _person(first="Madonna", last=None, middle=None)
    person.is_mononym = True
    record = Record("EHR_A", "EA1", person, 0)
    record.flags["mononym_style"] = style
    row = _render(ctx, record)
    assert (row["first_name"], row["last_name"]) == (first, last)


def test_unicode_variants(ctx):
    record = Record("EHR_A", "EA1", _person(first="José"), 0)
    assert _render(ctx, record)["first_name"] == "José"
    record.flags["unicode"] = "strip"
    assert _render(ctx, record)["first_name"] == "Jose"
    record.flags["unicode"] = "nfd"
    rendered = _render(ctx, record)["first_name"]
    assert rendered != "José" and unicodedata.normalize("NFC", rendered) == "José"


def test_migration_truncates_rotates_formats_and_fills_defaults(ctx):
    record = Record("EHR_A", "EA1", _person(first="Christopher", last="Venkataramanan"), 0)
    record.flags["ssn_missing"] = True
    record.flags["gender_missing"] = True
    row = _render(ctx, record, migrated=True)
    assert row["first_name"] == "CHRISTOPHE"
    assert row["last_name"] == "VENKATARAMAN"
    assert row["dob"] == "05/06/1980"  # iso -> us
    assert row["ssn"] == "000000000"  # migration default, digits format
    assert row["gender"] == "U"


def test_two_digit_year_and_year_only(ctx):
    record = Record("EHR_A", "EA1", _person(dob=date(1985, 6, 7)), 0)
    record.flags["dob_render"] = "two_digit_year"
    assert _render(ctx, record)["dob"] == "06/07/85"
    record.flags["dob_render"] = "year_only"
    assert _render(ctx, record)["dob"] == "1985"


# --------------------------------------------------------------------------- #
# Particles
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("surname", "expected"),
    [
        ("De la Cruz", ("De la", "Cruz")),
        ("Van der Berg", ("Van der", "Berg")),
        ("O'Brien", ("O'", "Brien")),
        ("Al-Rashid", ("Al", "Rashid")),
        ("Delgado", None),
        ("Smith", None),
    ],
)
def test_split_particle(ctx, surname, expected):
    assert split_particle(ctx, surname) == expected
