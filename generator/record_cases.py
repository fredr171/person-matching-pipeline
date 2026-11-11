"""Record-level edge cases: distortions applied when a source record is created.

Each case has an eligibility check and an apply step. The simulation applies
cases in catalog order: a case is applied with probability ``rate`` to records
that are eligible for it (D-016). A field holds at most one distortion, so an
SSN cannot have both a typo and a placeholder (D-020); distortions of
*different* fields stack freely.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import numpy as np

from generator import identifiers as ids
from generator.context import Context, name_key
from generator.model import Identity, Override, Record
from generator.rng import pick

Eligible = Callable[[Context, Record, Identity], bool]
Apply = Callable[[Context, Record, Identity, np.random.Generator], bool]

FIRST_NAME_PLACEHOLDERS = ("UNKNOWN", "FNU", "NFN", "BABY", "INFANT", "PATIENT", "ASDF")
LAST_NAME_PLACEHOLDERS = ("UNKNOWN", "LNU", "NLN", "ASDF", "XXX")
FULL_NAME_PLACEHOLDERS = (("TEST", "USER"), ("UNKNOWN", "UNKNOWN"), ("ASDF", "ASDF"))
GENDER_ERRORS = ("U", "X", "Q", "1", "?")


@dataclass(frozen=True)
class RecordCase:
    code: str
    eligible: Eligible
    apply: Apply


def _override(record: Record, field: str, value, base, code: str, until: int | None = None):
    record.overrides[field] = Override(value=value, base=base, code=code, until_load=until)


def _has_ssn(record: Record, identity: Identity) -> bool:
    return identity.ssn is not None and not record.flags.get("ssn_missing")


def _free(record: Record, *slots: str) -> bool:
    return not any(record.uses(slot) for slot in slots)


def _correction_load(ctx: Context, record: Record, rng: np.random.Generator) -> int:
    return int(rng.integers(record.created_load + 1, ctx.last_load + 1))


def _can_correct_later(ctx: Context, record: Record) -> bool:
    return record.created_load < ctx.last_load


def _title(name: str) -> str:
    return name.title()


# --------------------------------------------------------------------------- #
# SSN
# --------------------------------------------------------------------------- #


def _ssn_ok(ctx, record, identity):
    return _has_ssn(record, identity) and _free(record, "ssn")


def _ssn_distortion(code: str, distort) -> Apply:
    """Apply an SSN distortion, retrying until the result is valid and unused."""

    def apply(ctx, record, identity, rng):
        value = ctx.ssns.distorted(identity.ssn, lambda s: distort(rng, s))
        if value is None:
            return False
        _override(record, "ssn", value, identity.ssn, code)
        return True

    return apply


def _ssn_numeric_corrupt(ctx, record, identity, rng):
    kinds = ["float_suffix", "scientific"]
    if identity.ssn.startswith("0"):
        kinds.append("drop_leading_zeros")
    record.flags["ssn_corruption"] = pick(rng, kinds)
    record.occupy("ssn")
    return True


def _ssn_placeholder(ctx, record, identity, rng):
    _override(record, "ssn", pick(rng, ctx.ssn_placeholders), identity.ssn, "SSN_PLACEHOLDER")
    return True


def _ssn_temp_itin(ctx, record, identity, rng):
    _override(record, "ssn", ids.temporary_itin(rng), identity.ssn, "SSN_TEMP_ITIN")
    return True


def _ssn_delayed(ctx, record, identity, rng):
    load = _correction_load(ctx, record, rng)
    _override(record, "ssn", None, identity.ssn, "SSN_DELAYED", until=load)
    ctx.schedule_update(record, load)
    return True


def _ssn_corrected(ctx, record, identity, rng):
    load = _correction_load(ctx, record, rng)
    _override(record, "ssn", ctx.ssns.new(), identity.ssn, "SSN_CORRECTED", until=load)
    ctx.schedule_update(record, load)
    return True


# --------------------------------------------------------------------------- #
# DOB
# --------------------------------------------------------------------------- #


def _dob_ok(ctx, record, identity):
    return identity.dob is not None and _free(record, "dob")


def _today(ctx: Context, record: Record):
    return ctx.load_dates[record.created_load]


def _dob_value(code: str, compute) -> Apply:
    """Wrap a DOB distortion that may not apply to every date."""

    def apply(ctx, record, identity, rng):
        value = compute(ctx, record, identity, rng)
        if value is None or value == identity.dob:
            return False
        _override(record, "dob", value, identity.dob, code)
        return True

    return apply


def _dob_year_off_one(ctx, record, identity, rng):
    for years in (pick(rng, (-1, 1)), 1, -1):
        value = ids.shift_years(identity.dob, years)
        if value is not None and value < _today(ctx, record):
            return value
    return None


def _dob_leap_day(ctx, record, identity, rng):
    return (
        identity.dob.replace(day=28) if rng.random() < 0.5 else identity.dob.replace(month=3, day=1)
    )


def _is_leap_day(ctx, record, identity):
    return _dob_ok(ctx, record, identity) and (identity.dob.month, identity.dob.day) == (2, 29)


def _two_digit_year_ok(ctx, record, identity):
    # A naive parser reads YY as 20YY; only a future result is detectable.
    return (
        _dob_ok(ctx, record, identity) and 2000 + identity.dob.year % 100 > _today(ctx, record).year
    )


def _dob_render(style: str):
    def apply(ctx, record, identity, rng):
        record.flags["dob_render"] = style
        record.occupy("dob")
        return True

    return apply


# --------------------------------------------------------------------------- #
# Names
# --------------------------------------------------------------------------- #


def _name_fields(record: Record, identity: Identity, min_length: int = 1) -> list[str]:
    """Free name fields with a usable value, in a fixed order."""
    fields = []
    for field in ("first", "last"):
        value = identity.value(field)
        if value and len(value) >= min_length and _free(record, field):
            fields.append(field)
    return fields


def _typo(rng: np.random.Generator, name: str) -> str:
    letters = [i for i, c in enumerate(name) if c.isalpha()]
    position = pick(rng, letters)
    kind = pick(rng, ("substitute", "duplicate", "drop"))
    if kind == "duplicate":
        return name[: position + 1] + name[position] + name[position + 1 :]
    if kind == "drop" and len(letters) > 3:
        return name[:position] + name[position + 1 :]
    alphabet = "abcdefghijklmnopqrstuvwxyz"
    replacement = pick(rng, [c for c in alphabet if c != name[position].lower()])
    if name[position].isupper():
        replacement = replacement.upper()
    return name[:position] + replacement + name[position + 1 :]


def _name_typo(ctx, record, identity, rng):
    field = pick(rng, _name_fields(record, identity, min_length=3))
    value = identity.value(field)
    _override(record, field, _typo(rng, value), value, "NAME_TYPO")
    return True


def _nickname_ok(ctx, record, identity):
    return (
        not record.person.is_mononym
        and bool(identity.first)
        and _free(record, "first")
        and name_key(identity.first) in ctx.nicknames
    )


def _nickname(ctx, record, identity, rng):
    variant = pick(rng, ctx.nicknames[name_key(identity.first)])
    _override(record, "first", _title(variant), identity.first, "NAME_NICKNAME")
    return True


def _phonetic_fields(ctx, record, identity):
    return [
        f
        for f in _name_fields(record, identity)
        if name_key(identity.value(f)) in ctx.pools.phonetic
    ]


def _phonetic(ctx, record, identity, rng):
    field = pick(rng, _phonetic_fields(ctx, record, identity))
    value = identity.value(field)
    _override(record, field, pick(rng, ctx.pools.phonetic[name_key(value)]), value, "NAME_PHONETIC")
    return True


def _translit_fields(ctx, record, identity):
    return [
        f
        for f in _name_fields(record, identity)
        if name_key(identity.value(f)) in ctx.transliterations
    ]


def _transliteration(ctx, record, identity, rng):
    field = pick(rng, _translit_fields(ctx, record, identity))
    value = identity.value(field)
    variant = pick(rng, ctx.transliterations[name_key(value)])
    _override(record, field, _title(variant), value, "NAME_TRANSLITERATION")
    return True


def _order_ok(ctx, record, identity):
    return not record.person.is_mononym and bool(identity.last) and _free(record, "order")


def _swapped(ctx, record, identity, rng):
    record.flags["swapped"] = True
    record.occupy("order")
    return True


def _family_first_ok(ctx, record, identity):
    source = ctx.config.source(record.source_system)
    return (
        _order_ok(ctx, record, identity)
        and record.person.culture == "east_asian"
        and source.family_name_first
    )


def _family_first(ctx, record, identity, rng):
    record.flags["family_first"] = True
    record.occupy("order")
    return True


def _middle_ok(ctx, record, identity):
    return bool(identity.middle) and not record.person.is_mononym


def _middle_in_first(ctx, record, identity, rng):
    record.flags["middle_in_first"] = True
    return True


def _concatenated_ok(ctx, record, identity):
    multi_token = bool(identity.first) and " " in identity.first
    return multi_token or (record.flags.get("middle_in_first", False) and bool(identity.middle))


def _concatenated(ctx, record, identity, rng):
    record.flags["concatenated"] = True
    return True


def _initial_ok(ctx, record, identity):
    return bool(identity.first) and not record.person.is_mononym and _free(record, "first")


def _initial(ctx, record, identity, rng):
    initial = identity.first[0] + pick(rng, (".", ""))
    _override(record, "first", initial, identity.first, "NAME_INITIAL")
    return True


def _truncated(ctx, record, identity, rng):
    field = pick(rng, _name_fields(record, identity, min_length=8))
    value = identity.value(field)
    length = int(rng.integers(6, len(value)))
    _override(record, field, value[:length], value, "NAME_TRUNCATED")
    return True


def _compound_ok(ctx, record, identity):
    parts = record.person.compound_surname
    return parts is not None and identity.last == " ".join(parts) and _free(record, "last")


def _compound(ctx, record, identity, rng):
    part = pick(rng, record.person.compound_surname)
    _override(record, "last", part, identity.last, "NAME_COMPOUND_SURNAME")
    return True


def split_particle(ctx: Context, surname: str) -> tuple[str, str] | None:
    """Split "De la Cruz" into ("De la", "Cruz"); None when there is no particle."""
    key = name_key(surname)
    for particle in ctx.reference.particles_longest_first():
        if particle.endswith("'"):
            if key.startswith(particle) and len(key) > len(particle):
                return surname[: len(particle)], surname[len(particle) :]
            continue
        for separator in (" ", "-"):
            prefix = particle + separator
            if key.startswith(prefix) and len(key) > len(prefix):
                return surname[: len(particle)], surname[len(prefix) :]
    return None


def _particle_ok(ctx, record, identity):
    return (
        bool(identity.last)
        and _free(record, "last")
        and split_particle(ctx, identity.last) is not None
    )


def _particle(ctx, record, identity, rng):
    _, remainder = split_particle(ctx, identity.last)
    joined = "".join(c for c in identity.last if c not in " -'")
    value = joined if rng.random() < 0.5 else remainder
    _override(record, "last", value, identity.last, "NAME_PARTICLE")
    return True


def _unicode_ok(ctx, record, identity):
    names = (identity.first or "") + (identity.last or "")
    return not names.isascii()


def _unicode(ctx, record, identity, rng):
    record.flags["unicode"] = pick(rng, ("strip", "nfd"))
    return True


def _suffix_ok(ctx, record, identity):
    return bool(identity.suffix) and bool(identity.last)


def _suffix(ctx, record, identity, rng):
    record.flags["suffix_style"] = (int(rng.integers(4)), pick(rng, (" ", ", ")))
    return True


def _name_placeholder_ok(ctx, record, identity):
    return bool(_name_fields(record, identity)) and not record.person.is_mononym


def _name_placeholder(ctx, record, identity, rng):
    free = _name_fields(record, identity)
    mode = pick(rng, [*free, "both"] if len(free) == 2 else free)
    if mode == "both":
        first, last = pick(rng, FULL_NAME_PLACEHOLDERS)
        _override(record, "first", first, identity.first, "NAME_PLACEHOLDER")
        _override(record, "last", last, identity.last, "NAME_PLACEHOLDER")
    elif mode == "first":
        _override(
            record, "first", pick(rng, FIRST_NAME_PLACEHOLDERS), identity.first, "NAME_PLACEHOLDER"
        )
    else:
        _override(
            record, "last", pick(rng, LAST_NAME_PLACEHOLDERS), identity.last, "NAME_PLACEHOLDER"
        )
    return True


# --------------------------------------------------------------------------- #
# Gender
# --------------------------------------------------------------------------- #


def _gender_ok(ctx, record, identity):
    return not record.flags.get("gender_missing") and _free(record, "gender")


def _gender_error(ctx, record, identity, rng):
    other = {"M": "F", "F": "M"}.get(identity.gender, "M")
    _override(
        record,
        "gender",
        pick(rng, (other, other, *GENDER_ERRORS)),
        identity.gender,
        "GENDER_CHANGE_OR_ERROR",
    )
    return True


# --------------------------------------------------------------------------- #
# Registry (catalog order)
# --------------------------------------------------------------------------- #


def _case(code: str, eligible: Eligible, apply: Apply) -> RecordCase:
    return RecordCase(code, eligible, apply)


RECORD_CASES: tuple[RecordCase, ...] = (
    _case("SSN_TYPO_SUB", _ssn_ok, _ssn_distortion("SSN_TYPO_SUB", ids.ssn_substitute_digit)),
    _case(
        "SSN_TYPO_TRANSPOSE",
        _ssn_ok,
        _ssn_distortion("SSN_TYPO_TRANSPOSE", ids.ssn_transpose_adjacent),
    ),
    _case("SSN_NUMERIC_CORRUPT", _ssn_ok, _ssn_numeric_corrupt),
    _case("SSN_PLACEHOLDER", _ssn_ok, _ssn_placeholder),
    _case("SSN_TEMP_ITIN", _ssn_ok, _ssn_temp_itin),
    _case(
        "SSN_DELAYED",
        lambda c, r, i: _ssn_ok(c, r, i) and _can_correct_later(c, r),
        _ssn_delayed,
    ),
    _case(
        "SSN_CORRECTED",
        lambda c, r, i: _ssn_ok(c, r, i) and _can_correct_later(c, r),
        _ssn_corrected,
    ),
    _case(
        "DOB_TYPO_DIGIT",
        _dob_ok,
        _dob_value("DOB_TYPO_DIGIT", lambda c, r, i, g: ids.dob_digit_typo(g, i.dob, _today(c, r))),
    ),
    _case(
        "DOB_MONTH_DAY_SWAP",
        lambda c, r, i: _dob_ok(c, r, i) and ids.dob_month_day_swap(i.dob) is not None,
        _dob_value("DOB_MONTH_DAY_SWAP", lambda c, r, i, g: ids.dob_month_day_swap(i.dob)),
    ),
    _case("DOB_YEAR_OFF_ONE", _dob_ok, _dob_value("DOB_YEAR_OFF_ONE", _dob_year_off_one)),
    _case("DOB_LEAP_DAY", _is_leap_day, _dob_value("DOB_LEAP_DAY", _dob_leap_day)),
    _case(
        "DOB_PLACEHOLDER",
        _dob_ok,
        _dob_value("DOB_PLACEHOLDER", lambda c, r, i, g: pick(g, c.dob_placeholders)),
    ),
    _case(
        "DOB_JAN_FIRST",
        lambda c, r, i: _dob_ok(c, r, i) and (i.dob.month, i.dob.day) != (1, 1),
        _dob_value("DOB_JAN_FIRST", lambda c, r, i, g: i.dob.replace(month=1, day=1)),
    ),
    _case("DOB_TWO_DIGIT_YEAR", _two_digit_year_ok, _dob_render("two_digit_year")),
    _case("DOB_YEAR_ONLY", _dob_ok, _dob_render("year_only")),
    _case("NAME_TYPO", lambda c, r, i: bool(_name_fields(r, i, 3)), _name_typo),
    _case("NAME_NICKNAME", _nickname_ok, _nickname),
    _case("NAME_PHONETIC", lambda c, r, i: bool(_phonetic_fields(c, r, i)), _phonetic),
    _case(
        "NAME_TRANSLITERATION", lambda c, r, i: bool(_translit_fields(c, r, i)), _transliteration
    ),
    _case("NAME_SWAPPED", _order_ok, _swapped),
    _case("NAME_FAMILY_FIRST", _family_first_ok, _family_first),
    _case("NAME_MIDDLE_IN_FIRST", _middle_ok, _middle_in_first),
    _case("NAME_CONCATENATED", _concatenated_ok, _concatenated),
    _case("NAME_INITIAL", _initial_ok, _initial),
    _case("NAME_TRUNCATED", lambda c, r, i: bool(_name_fields(r, i, 8)), _truncated),
    _case("NAME_COMPOUND_SURNAME", _compound_ok, _compound),
    _case("NAME_PARTICLE", _particle_ok, _particle),
    _case("NAME_UNICODE", _unicode_ok, _unicode),
    _case("NAME_SUFFIX_IN_LAST", _suffix_ok, _suffix),
    _case("NAME_PLACEHOLDER", _name_placeholder_ok, _name_placeholder),
    _case("GENDER_CHANGE_OR_ERROR", _gender_ok, _gender_error),
)


def apply_record_cases(ctx: Context, record: Record, rng: np.random.Generator) -> None:
    """Apply every eligible record-level case with its configured probability."""
    identity = record.person.identity_at(record.created_load, record.created_load)
    for case in RECORD_CASES:
        rate = ctx.rate(case.code)
        if rate <= 0 or not case.eligible(ctx, record, identity):
            continue
        if rng.random() < rate and case.apply(ctx, record, identity, rng):
            record.tag(case.code, record.created_load)
