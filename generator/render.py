"""Render a record into the raw strings a source system would emit.

The order of steps matters and is fixed:

1. Take the person's identity as of the load.
2. Apply the record's active overrides (typos, nicknames, placeholders, ...).
3. Assemble names: middle name in the first name, concatenation, mononyms,
   generational suffix in the last name, first/last swaps.
4. Apply Unicode variation (accents stripped, or decomposed form).
5. Apply the source's casing and, after a migration, its field lengths.
6. Format SSN, DOB and gender the way the source writes them.
"""

from __future__ import annotations

import unicodedata
from dataclasses import dataclass

from generator.identifiers import format_dob, format_ssn, numeric_corruption
from generator.model import Record
from pipeline.config import DOB_FORMATS, SSN_FORMATS, Migration, SourceSystem

# How generational suffixes are written into last names (NAME_SUFFIX_IN_LAST).
SUFFIX_SPELLINGS = {
    "JR": ("Jr", "Jr.", "JR", "Junior"),
    "SR": ("Sr", "Sr.", "SR", "Senior"),
    "II": ("II", "2nd"),
    "III": ("III", "3rd"),
    "IV": ("IV", "4th"),
}
SUFFIX_SEPARATORS = (" ", ", ")

EVENT_VALUE_COLUMNS = ("first_name", "last_name", "dob", "ssn", "gender")


@dataclass(slots=True)
class SourceState:
    """A source system's current formatting habits; changes on migration."""

    config: SourceSystem
    name_case: str
    dob_format: str
    ssn_format: str
    migrated: bool = False

    @classmethod
    def initial(cls, config: SourceSystem) -> SourceState:
        return cls(config, config.name_case, config.dob_format, config.ssn_format)

    def migrate(self, migration: Migration) -> None:
        """Switch to the post-migration formats (MIGRATION_REFORMAT)."""
        self.name_case = migration.name_case
        self.dob_format = DOB_FORMATS[(DOB_FORMATS.index(self.dob_format) + 1) % 3]
        self.ssn_format = SSN_FORMATS[(SSN_FORMATS.index(self.ssn_format) + 1) % 3]
        self.migrated = True


def strip_accents(text: str) -> str:
    decomposed = unicodedata.normalize("NFD", text)
    return unicodedata.normalize(
        "NFC", "".join(c for c in decomposed if unicodedata.category(c) != "Mn")
    )


def apply_case(text: str, style: str) -> str:
    if style == "upper":
        return text.upper()
    if style == "lower":
        return text.lower()
    return text.title()


def render(
    record: Record, load_id: int, source: SourceState, migration: Migration
) -> dict[str, str | None]:
    """Raw column values for an insert or update event of ``record`` at ``load_id``."""
    person = record.person
    identity = person.identity_at(load_id, created_load=record.created_load)

    values = {}
    for name in ("first", "last", "ssn", "dob", "gender"):
        value = identity.value(name)
        override = record.overrides.get(name)
        if override is not None and override.active(load_id, value):
            value = override.value
        values[name] = value

    first, last = values["first"], values["last"]
    flags = record.flags

    # Name assembly.
    if flags.get("middle_in_first") and identity.middle and first:
        first = f"{first} {identity.middle}"
    if flags.get("concatenated") and first:
        first = first.replace(" ", "")
    if person.is_mononym:
        style = flags.get("mononym_style", "repeat")
        if style == "fnu":
            first, last = "FNU", first
        elif style == "lnu":
            last = "LNU"
        else:
            last = first
    suffix_style = flags.get("suffix_style")
    if suffix_style and identity.suffix and last:
        spelling_index, separator = suffix_style
        spellings = SUFFIX_SPELLINGS[identity.suffix]
        last = f"{last}{separator}{spellings[spelling_index % len(spellings)]}"
    if flags.get("swapped") or flags.get("family_first"):
        first, last = last, first

    # Unicode variation, casing, migration limits.
    names = [first, last]
    unicode_style = flags.get("unicode")
    case = flags.get("case") if source.name_case == "mixed" else source.name_case
    for i, name in enumerate(names):
        if name is None:
            continue
        if unicode_style == "strip":
            name = strip_accents(name)
        name = apply_case(name, case or "title")
        if unicode_style == "nfd":
            name = unicodedata.normalize("NFD", name)
        if source.migrated:
            limit = migration.first_name_max_length if i == 0 else migration.last_name_max_length
            name = name[:limit]
        names[i] = name
    first, last = names

    # SSN.
    ssn = values["ssn"]
    if flags.get("ssn_missing"):
        ssn = None
    if ssn is None and source.migrated:
        ssn = migration.default_ssn
    if ssn is None:
        ssn_text = ""
    elif flags.get("ssn_corruption") and len(ssn) == 9 and ssn.isdigit():
        ssn_text = numeric_corruption(ssn, flags["ssn_corruption"])
    else:
        ssn_text = format_ssn(ssn, source.ssn_format) if ssn.isdigit() and len(ssn) == 9 else ssn

    # DOB.
    dob = values["dob"]
    dob_style = flags.get("dob_render")
    if dob is None:
        dob_text = ""
    elif dob_style == "two_digit_year":
        dob_text = f"{dob.month:02d}/{dob.day:02d}/{dob.year % 100:02d}"
    elif dob_style == "year_only":
        dob_text = f"{dob.year:04d}"
    else:
        dob_text = format_dob(dob, source.dob_format)

    # Gender.
    gender = values["gender"]
    if flags.get("gender_missing"):
        gender = source.config.gender_unknown_value
    if not gender and source.migrated:
        gender = migration.default_gender

    return {
        "first_name": first or "",
        "last_name": last or "",
        "dob": dob_text,
        "ssn": ssn_text,
        "gender": gender or "",
    }
