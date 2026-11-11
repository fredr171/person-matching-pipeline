"""SSN and date-of-birth generation, distortion and formatting.

SSNs are kept internally as nine-digit strings and DOBs as ``datetime.date``.
Distortions return new values; formatting into a source's raw string happens
last, in :mod:`generator.render`.

Generated SSNs are valid in format only. They are random numbers, not linked to
any real person, and generated data is never committed (DESIGN.md section 19).
"""

from __future__ import annotations

import calendar
from datetime import date

import numpy as np

from generator.rng import pick

# --------------------------------------------------------------------------- #
# SSNs
# --------------------------------------------------------------------------- #


def is_valid_ssn(ssn: str) -> bool:
    """Valid format per DESIGN.md section 5: not area 000/666/9xx, group 00, serial 0000."""
    if len(ssn) != 9 or not ssn.isdigit():
        return False
    area, group, serial = ssn[:3], ssn[3:5], ssn[5:]
    return area not in {"000", "666"} and area[0] != "9" and group != "00" and serial != "0000"


class SsnAllocator:
    """Hands out unique, valid-format SSNs, never a placeholder value."""

    def __init__(self, rng: np.random.Generator, placeholders: frozenset[str]) -> None:
        self._rng = rng
        self._used: set[str] = set(placeholders)

    def is_used(self, ssn: str) -> bool:
        return ssn in self._used

    def reserve(self, ssn: str) -> bool:
        """Mark ``ssn`` as taken; False if it already was or is invalid."""
        if ssn in self._used or not is_valid_ssn(ssn):
            return False
        self._used.add(ssn)
        return True

    def new(self) -> str:
        while True:
            area = int(self._rng.integers(1, 900))
            group = int(self._rng.integers(1, 100))
            serial = int(self._rng.integers(1, 10000))
            ssn = f"{area:03d}{group:02d}{serial:04d}"
            if self.reserve(ssn):
                return ssn

    def near(self, base: str) -> str:
        """A sibling's SSN: same area and group, nearby serial (twins, multiples)."""
        serial = int(base[5:])
        for offset in (1, 2, 3, -1, -2, -3, 4, 5, 6, 7):
            candidate = serial + offset
            if 1 <= candidate <= 9999:
                ssn = f"{base[:5]}{candidate:04d}"
                if self.reserve(ssn):
                    return ssn
        return self.new()

    def distorted(self, base: str, distort) -> str | None:
        """Apply ``distort`` until it yields a valid SSN nobody holds, or give up."""
        for _ in range(20):
            candidate = distort(base)
            if candidate != base and is_valid_ssn(candidate) and candidate not in self._used:
                return candidate
        return None


def ssn_substitute_digit(rng: np.random.Generator, ssn: str) -> str:
    position = int(rng.integers(9))
    digit = str((int(ssn[position]) + int(rng.integers(1, 10))) % 10)
    return ssn[:position] + digit + ssn[position + 1 :]


def ssn_transpose_adjacent(rng: np.random.Generator, ssn: str) -> str:
    positions = [i for i in range(8) if ssn[i] != ssn[i + 1]]
    if not positions:
        return ssn
    i = pick(rng, positions)
    return ssn[:i] + ssn[i + 1] + ssn[i] + ssn[i + 2 :]


def temporary_itin(rng: np.random.Generator) -> str:
    """ITIN-style number: starts with 9, group 70-88."""
    area = int(rng.integers(0, 100))
    group = int(rng.integers(70, 89))
    serial = int(rng.integers(1, 10000))
    return f"9{area:02d}{group:02d}{serial:04d}"


def format_ssn(ssn: str, style: str) -> str:
    if style == "dashed":
        return f"{ssn[:3]}-{ssn[3:5]}-{ssn[5:]}"
    if style == "spaced":
        return f"{ssn[:3]} {ssn[3:5]} {ssn[5:]}"
    return ssn


def numeric_corruption(ssn: str, kind: str) -> str:
    """Render an SSN the way a numeric column would (SSN_NUMERIC_CORRUPT).

    All three forms are recoverable without loss: dropped leading zeros are
    restored by left-padding, ``.0`` is dropped, and the scientific form keeps
    every digit.
    """
    number = int(ssn)
    if kind == "drop_leading_zeros":
        return str(number)
    if kind == "float_suffix":
        return f"{number}.0"
    if kind == "scientific":
        return f"{number:.8E}"
    raise ValueError(f"Unknown SSN corruption {kind!r}")


# --------------------------------------------------------------------------- #
# Dates of birth
# --------------------------------------------------------------------------- #


def add_months(day: date, months: int) -> date:
    month_index = day.month - 1 + months
    year, month = day.year + month_index // 12, month_index % 12 + 1
    return date(year, month, min(day.day, calendar.monthrange(year, month)[1]))


def shift_years(day: date, years: int) -> date | None:
    """Same month/day ``years`` later, or None when that date does not exist (Feb 29)."""
    try:
        return day.replace(year=day.year + years)
    except ValueError:
        return None


def dob_digit_typo(rng: np.random.Generator, dob: date, today: date) -> date | None:
    """Change one digit of YYYYMMDD while keeping a valid, past date."""
    text = dob.strftime("%Y%m%d")
    for _ in range(30):
        position = int(rng.integers(8))
        digit = str((int(text[position]) + int(rng.integers(1, 10))) % 10)
        candidate = text[:position] + digit + text[position + 1 :]
        try:
            value = date(int(candidate[:4]), int(candidate[4:6]), int(candidate[6:]))
        except ValueError:
            continue
        if value != dob and value < today and abs(value.year - dob.year) <= 10:
            return value
    return None


def dob_month_day_swap(dob: date) -> date | None:
    if dob.day > 12 or dob.day == dob.month:
        return None
    return date(dob.year, dob.day, dob.month)


def format_dob(dob: date, style: str) -> str:
    if style == "us":
        return f"{dob.month:02d}/{dob.day:02d}/{dob.year:04d}"
    if style == "compact":
        return f"{dob.year:04d}{dob.month:02d}{dob.day:02d}"
    return f"{dob.year:04d}-{dob.month:02d}-{dob.day:02d}"
