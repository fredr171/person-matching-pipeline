"""Typed loaders for the reference data in ``config/reference/``.

Both matching implementations read these same CSV files, so they cannot drift
apart (DESIGN.md section 2). dbt reads them as external sources with every
column typed as text (``matching_dbt/models/staging/sources.yml``); Python and
PySpark read them through this module.

All values are stored in their *standardized* form (uppercase, accents removed,
punctuation removed except apostrophes), so lookups compare standardized names
with standardized reference values.

Files are read with :mod:`csv` rather than pandas on purpose: pandas turns
strings such as ``NA`` and ``NULL`` into missing values by default (verified;
see D-018), and those strings are exactly what ``placeholders.csv`` lists.
"""

from __future__ import annotations

import csv
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from pipeline import paths

NICKNAME_RELATIONS: frozenset[str] = frozenset({"nickname", "transliteration", "spelling"})
GENDERS: frozenset[str] = frozenset({"M", "F", "U"})
PLACEHOLDER_FIELDS: frozenset[str] = frozenset({"name", "ssn", "dob"})
STANDARD_GENDERS: frozenset[str] = frozenset({"M", "F", "X"})


class ReferenceDataError(ValueError):
    """Raised when a reference CSV has the wrong columns or invalid values."""


@dataclass(frozen=True)
class NameVariant:
    """One row of ``nicknames.csv``: ``variant`` is an alternative form of ``canonical``.

    ``canonical_gender`` is the usual gender of the canonical name (``U`` for
    surnames and unisex names). It is used to derive gender-ambiguous variants
    (D-015).
    """

    canonical: str
    canonical_gender: str
    variant: str
    relation: str


@dataclass(frozen=True)
class Placeholder:
    """One row of ``placeholders.csv``: a value that means "unknown" for a field."""

    field: str
    value: str
    reason: str


@dataclass(frozen=True)
class ReferenceData:
    """All reference tables, loaded together."""

    name_variants: tuple[NameVariant, ...]
    placeholders: tuple[Placeholder, ...]
    particles: tuple[str, ...]
    suffixes: dict[str, str]
    letters: dict[str, str]
    genders: dict[str, str]

    def placeholder_values(self, field: str) -> frozenset[str]:
        """All placeholder values for ``name``, ``ssn`` or ``dob``."""
        if field not in PLACEHOLDER_FIELDS:
            raise ValueError(f"Unknown placeholder field {field!r}")
        return frozenset(p.value for p in self.placeholders if p.field == field)

    def gender_ambiguous_variants(self) -> frozenset[str]:
        """Variants that map to canonical names of both genders (e.g. CHRIS, PAT).

        DESIGN.md section 7: a nickname match through one of these, between
        records with conflicting non-null genders, is downgraded to WEAK.
        """
        genders_by_variant: dict[str, set[str]] = defaultdict(set)
        for row in self.name_variants:
            genders_by_variant[row.variant].add(row.canonical_gender)
        return frozenset(v for v, g in genders_by_variant.items() if {"M", "F"} <= g)

    def particles_longest_first(self) -> tuple[str, ...]:
        """Particles ordered so multi-word particles are tried before their prefixes.

        ``DE LA`` must be stripped before ``DE``, otherwise "DE LA CRUZ" would
        leave "LA CRUZ".
        """
        return tuple(sorted(self.particles, key=lambda p: (-len(p), p)))


def _read_csv(path: Path, expected_columns: list[str]) -> list[dict[str, str]]:
    if not path.is_file():
        raise ReferenceDataError(f"Reference file not found: {path}")
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != expected_columns:
            raise ReferenceDataError(
                f"{path.name}: expected columns {expected_columns}, got {reader.fieldnames}"
            )
        rows = list(reader)
    for line_number, row in enumerate(rows, start=2):
        for column in expected_columns:
            value = row[column]
            if value is None or value.strip() == "":
                raise ReferenceDataError(f"{path.name} line {line_number}: empty {column!r}")
            if value != value.strip():
                raise ReferenceDataError(
                    f"{path.name} line {line_number}: {column!r} has surrounding whitespace"
                )
    return rows


def _check_unique(items: list[tuple[str, ...]], filename: str) -> None:
    seen: set[tuple[str, ...]] = set()
    for item in items:
        if item in seen:
            raise ReferenceDataError(f"{filename}: duplicate entry {item}")
        seen.add(item)


def load_name_variants(path: Path | None = None) -> tuple[NameVariant, ...]:
    """Load ``nicknames.csv`` (nicknames, transliterations and spelling variants)."""
    path = path or paths.REFERENCE_DIR / "nicknames.csv"
    rows = _read_csv(path, ["canonical", "canonical_gender", "variant", "relation"])
    variants = tuple(NameVariant(**row) for row in rows)
    for v in variants:
        if v.canonical_gender not in GENDERS:
            raise ReferenceDataError(f"nicknames.csv: bad gender {v.canonical_gender!r}")
        if v.relation not in NICKNAME_RELATIONS:
            raise ReferenceDataError(f"nicknames.csv: bad relation {v.relation!r}")
        if v.canonical == v.variant:
            raise ReferenceDataError(f"nicknames.csv: {v.canonical} maps to itself")
    _check_unique([(v.canonical, v.variant) for v in variants], "nicknames.csv")
    return variants


def load_placeholders(path: Path | None = None) -> tuple[Placeholder, ...]:
    """Load ``placeholders.csv`` (values that mean "unknown" for names, SSNs and DOBs)."""
    path = path or paths.REFERENCE_DIR / "placeholders.csv"
    placeholders = tuple(
        Placeholder(**row) for row in _read_csv(path, ["field", "value", "reason"])
    )
    for p in placeholders:
        if p.field not in PLACEHOLDER_FIELDS:
            raise ReferenceDataError(f"placeholders.csv: bad field {p.field!r}")
    _check_unique([(p.field, p.value) for p in placeholders], "placeholders.csv")
    return placeholders


def load_particles(path: Path | None = None) -> tuple[str, ...]:
    """Load ``particles.csv`` (surname particles such as DE LA, VAN DER, O')."""
    path = path or paths.REFERENCE_DIR / "particles.csv"
    particles = tuple(row["particle"] for row in _read_csv(path, ["particle"]))
    _check_unique([(p,) for p in particles], "particles.csv")
    return particles


def load_suffixes(path: Path | None = None) -> dict[str, str]:
    """Load ``suffixes.csv`` as a mapping from written form to canonical suffix."""
    path = path or paths.REFERENCE_DIR / "suffixes.csv"
    rows = _read_csv(path, ["form", "canonical"])
    _check_unique([(row["form"],) for row in rows], "suffixes.csv")
    return {row["form"]: row["canonical"] for row in rows}


def load_letters(path: Path | None = None) -> dict[str, str]:
    """Load ``letters.csv``: accented or special Latin letters mapped to ASCII.

    Accent stripping is table-driven so that DuckDB and Spark, which have no
    common accent-stripping function, can reproduce it exactly (D-029).
    """
    path = path or paths.REFERENCE_DIR / "letters.csv"
    rows = _read_csv(path, ["character", "replacement"])
    for row in rows:
        if len(row["character"]) != 1 or row["character"].isascii():
            raise ReferenceDataError(f"letters.csv: {row['character']!r} is not one non-ASCII char")
        if not (row["replacement"].isascii() and row["replacement"].isupper()):
            raise ReferenceDataError(f"letters.csv: bad replacement {row['replacement']!r}")
    _check_unique([(row["character"],) for row in rows], "letters.csv")
    return {row["character"]: row["replacement"] for row in rows}


def load_genders(path: Path | None = None) -> dict[str, str]:
    """Load ``genders.csv``: raw gender values mapped to M, F or X. Others become null."""
    path = path or paths.REFERENCE_DIR / "genders.csv"
    rows = _read_csv(path, ["value", "gender"])
    for row in rows:
        if row["gender"] not in STANDARD_GENDERS:
            raise ReferenceDataError(f"genders.csv: bad gender {row['gender']!r}")
        if row["value"] != row["value"].upper():
            raise ReferenceDataError(f"genders.csv: value {row['value']!r} must be uppercase")
    _check_unique([(row["value"],) for row in rows], "genders.csv")
    return {row["value"]: row["gender"] for row in rows}


def load_reference_data(reference_dir: Path = paths.REFERENCE_DIR) -> ReferenceData:
    """Load every reference table from ``reference_dir``."""
    return ReferenceData(
        name_variants=load_name_variants(reference_dir / "nicknames.csv"),
        placeholders=load_placeholders(reference_dir / "placeholders.csv"),
        particles=load_particles(reference_dir / "particles.csv"),
        suffixes=load_suffixes(reference_dir / "suffixes.csv"),
        letters=load_letters(reference_dir / "letters.csv"),
        genders=load_genders(reference_dir / "genders.csv"),
    )
