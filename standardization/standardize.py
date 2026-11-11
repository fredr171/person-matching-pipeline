"""Field standardization (DESIGN.md section 5): the reference both engines must match.

Every function here is written so that it can be expressed in SQL (DuckDB)
and in Spark DataFrame operations: regular expressions, table lookups and
string functions only. Raw values are kept alongside the cleaned ones.

Names
    1. Unicode NFC, then map accented and special letters to ASCII through
       ``letters.csv``; drop any remaining combining marks (D-029). Engines
       without NFC get the same result by mapping and then dropping marks.
    2. Remove any other non-ASCII character; uppercase.
    3. Typographic apostrophes become ``'``. Everything except letters,
       digits, spaces, hyphens and apostrophes becomes a space; a hyphen or
       apostrophe that is not between two letters or digits becomes a space.
    4. Collapse whitespace.
    5. A trailing suffix token (JR, SR, II, ... in any form from
       ``suffixes.csv``) moves to ``suffix``, if another token remains.
    6. A value listed in ``placeholders.csv`` (field ``name``) becomes null.

    Comparison forms (D-030) are then derived from the cleaned name: the name
    itself; without apostrophes and with hyphens as spaces; that with spaces
    removed; the same three with a leading particle stripped; and each token
    of two or more letters that is not a particle word.

SSN
    Numeric artifacts are recovered first (``.0`` dropped; scientific notation
    expanded when every digit is present), and only such purely numeric values
    are left-padded to 9 digits. Formatted values keep digits only and must
    then have exactly 9. Placeholders, area 000/666, areas starting with 9
    (ITINs and temporary IDs), group 00 and serial 0000 become null.

DOB
    ISO ``YYYY-MM-DD`` (optionally with a time), US ``MM/DD/YYYY``, US
    ``MM/DD/YY`` (read as 20YY, as a naive parser would), and ``YYYYMMDD``.
    Year-only values, impossible dates, placeholders and dates after the load
    date become null. A January 1 date is kept and flagged ``dob_is_jan1``.

Gender
    Mapped through ``genders.csv`` to M, F or X; anything else is null.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import date

from pipeline.reference import ReferenceData

# --------------------------------------------------------------------------- #
# Lookups derived from the reference data
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class StandardizationTables:
    """The reference lookups standardization needs, built once."""

    letters: dict[str, str]
    suffixes: dict[str, str]
    particles: tuple[str, ...]  # longest first
    particle_words: frozenset[str]
    name_placeholders: frozenset[str]
    ssn_placeholders: frozenset[str]
    dob_placeholders: frozenset[str]
    genders: dict[str, str]

    @classmethod
    def from_reference(cls, ref: ReferenceData) -> StandardizationTables:
        particles = ref.particles_longest_first()
        words = {w for p in particles if not p.endswith("'") for w in p.split(" ")}
        return cls(
            letters=dict(ref.letters),
            suffixes=dict(ref.suffixes),
            particles=particles,
            particle_words=frozenset(words),
            name_placeholders=ref.placeholder_values("name"),
            ssn_placeholders=ref.placeholder_values("ssn"),
            dob_placeholders=ref.placeholder_values("dob"),
            genders=dict(ref.genders),
        )


# --------------------------------------------------------------------------- #
# Names
# --------------------------------------------------------------------------- #

TYPOGRAPHIC_APOSTROPHES = "’‘ʼ`´"
_COMBINING_MARKS = re.compile("[̀-ͯ]")
_NON_ASCII = re.compile("[^\u0000-\u007f]")
_NOT_NAME_CHARACTER = re.compile(r"[^A-Z0-9' -]")
# A hyphen or apostrophe survives only between two letters or digits. Written
# as three lookaround-free steps so RE2 (DuckDB) and Java (Spark) agree (D-039):
# runs of marks go, then marks after a non-alphanumeric, then marks before one.
_MARK_RUN = re.compile(r"['-]{2,}")
_MARK_AFTER_GAP = re.compile(r"(^|[^A-Z0-9])['-]")
_MARK_BEFORE_GAP = re.compile(r"['-]($|[^A-Z0-9])")
_SPACES = re.compile(r" +")
# Whitespace trimmed from raw SSN, DOB and gender values (ASCII only, like SQL trim).
RAW_WHITESPACE = " \t\r\n"


def _collapse(text: str) -> str:
    return _SPACES.sub(" ", text).strip(" ")


def clean_name(value: str | None, letters: dict[str, str]) -> str | None:
    """Steps 1-4: normalized, ASCII, uppercase name text, or None if nothing is left."""
    if value is None:
        return None
    text = unicodedata.normalize("NFC", value)
    text = "".join(letters.get(char, char) for char in text)
    text = _COMBINING_MARKS.sub("", text)
    for mark in TYPOGRAPHIC_APOSTROPHES:
        text = text.replace(mark, "'")
    text = _NON_ASCII.sub(" ", text).upper()
    text = _NOT_NAME_CHARACTER.sub(" ", text)
    text = _MARK_RUN.sub(" ", text)
    text = _MARK_AFTER_GAP.sub(r"\1 ", text)
    text = _MARK_BEFORE_GAP.sub(r" \1", text)
    return _collapse(text) or None


def split_suffix(text: str | None, suffixes: dict[str, str]) -> tuple[str | None, str | None]:
    """Step 5: move a trailing suffix token into a separate value."""
    if text is None:
        return None, None
    tokens = text.split(" ")
    if len(tokens) > 1 and tokens[-1] in suffixes:
        return " ".join(tokens[:-1]), suffixes[tokens[-1]]
    return text, None


def strip_particle(text: str, particles: tuple[str, ...]) -> str | None:
    """The name without its leading particle, or None if it has none.

    ``particles`` must be longest first, so "DE LA CRUZ" loses "DE LA", not "DE".
    A particle ending in an apostrophe (O') is attached to the name.
    """
    for particle in particles:
        if particle.endswith("'"):
            if text.startswith(particle) and len(text) > len(particle):
                return text[len(particle) :]
        elif text.startswith(particle + " "):
            return text[len(particle) + 1 :]
    return None


def _plain(text: str) -> str:
    """Without apostrophes, with hyphens as spaces."""
    return _collapse(text.replace("'", "").replace("-", " "))


def name_forms(text: str | None, tables: StandardizationTables) -> tuple[str, ...]:
    """All comparison forms of a cleaned name, sorted (D-030)."""
    if text is None:
        return ()
    bases = [text]
    stripped = strip_particle(text, tables.particles)
    if stripped:
        bases.append(stripped)
    forms = set()
    for base in bases:
        plain = _plain(base)
        forms.update({base, plain, plain.replace(" ", "")})
    tokens = [t for t in _plain(text).split(" ") if len(t) > 1 and t not in tables.particle_words]
    if len(tokens) > 1:
        forms.update(tokens)
    return tuple(sorted(f for f in forms if f))


# --------------------------------------------------------------------------- #
# SSN
# --------------------------------------------------------------------------- #

_SCIENTIFIC = re.compile(r"^([0-9])(?:\.([0-9]+))?E\+?([0-9]+)$")
_FLOAT_ZERO = re.compile(r"^([0-9]+)\.0+$")
_DIGITS_ONLY = re.compile(r"^[0-9]+$")
_NON_DIGIT = re.compile(r"[^0-9]")


def _expand_scientific(text: str) -> str | None:
    """Digits of a number in scientific notation, or None if digits were lost."""
    match = _SCIENTIFIC.match(text)
    lead, fraction, exponent = match.group(1), match.group(2) or "", int(match.group(3))
    if len(fraction) < exponent:
        return None  # trailing digits were rounded away: not lossless
    if fraction[exponent:].strip("0"):
        return None  # a fractional part: not an integer
    return lead + fraction[:exponent]


def standardize_ssn(raw: str | None, placeholders: frozenset[str]) -> tuple[str | None, str | None]:
    """Return ``(ssn, null_reason)``; exactly one of the two is None."""
    if raw is None or raw.strip(RAW_WHITESPACE) == "":
        return None, "MISSING"
    text = raw.strip(RAW_WHITESPACE).upper()
    if _SCIENTIFIC.match(text):
        digits = _expand_scientific(text)
        if digits is None:
            return None, "UNRECOVERABLE"
        numeric = True
    elif match := _FLOAT_ZERO.match(text):
        digits, numeric = match.group(1), True
    else:
        digits, numeric = _NON_DIGIT.sub("", text), bool(_DIGITS_ONLY.match(text))
    if not digits:
        return None, "UNPARSEABLE"
    if len(digits) > 9 or (len(digits) < 9 and not numeric):
        return None, "INVALID_LENGTH"
    ssn = digits.zfill(9)
    if ssn in placeholders:
        return None, "PLACEHOLDER"
    if ssn.startswith("9"):
        return None, "ITIN_OR_TEMPORARY"
    if ssn[:3] in ("000", "666"):
        return None, "INVALID_AREA"
    if ssn[3:5] == "00":
        return None, "INVALID_GROUP"
    if ssn[5:] == "0000":
        return None, "INVALID_SERIAL"
    return ssn, None


# --------------------------------------------------------------------------- #
# DOB
# --------------------------------------------------------------------------- #

_DOB_ISO = re.compile(r"^([0-9]{4})-([0-9]{1,2})-([0-9]{1,2})(?:[ T].*)?$")
_DOB_US = re.compile(r"^([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})$")
_DOB_US_TWO_DIGIT = re.compile(r"^([0-9]{1,2})/([0-9]{1,2})/([0-9]{2})$")
_DOB_COMPACT = re.compile(r"^([0-9]{4})([0-9]{2})([0-9]{2})$")
_DOB_YEAR_ONLY = re.compile(r"^[0-9]{4}$")


def _year_month_day(text: str) -> tuple[int, int, int] | None:
    if match := _DOB_ISO.match(text):
        return int(match.group(1)), int(match.group(2)), int(match.group(3))
    if match := _DOB_US.match(text):
        return int(match.group(3)), int(match.group(1)), int(match.group(2))
    if match := _DOB_US_TWO_DIGIT.match(text):
        return 2000 + int(match.group(3)), int(match.group(1)), int(match.group(2))
    if match := _DOB_COMPACT.match(text):
        return int(match.group(1)), int(match.group(2)), int(match.group(3))
    return None


def standardize_dob(
    raw: str | None, placeholders: frozenset[str], today: date
) -> tuple[date | None, str | None]:
    """Return ``(dob, null_reason)``; exactly one of the two is None."""
    if raw is None or raw.strip(RAW_WHITESPACE) == "":
        return None, "MISSING"
    text = raw.strip(RAW_WHITESPACE)
    if _DOB_YEAR_ONLY.match(text):
        return None, "YEAR_ONLY"
    parts = _year_month_day(text)
    if parts is None:
        return None, "UNPARSEABLE"
    try:
        dob = date(*parts)  # year 0 is rejected here, and by a guard in SQL
    except ValueError:
        return None, "INVALID_DATE"
    if dob.isoformat() in placeholders:
        return None, "PLACEHOLDER"
    if dob > today:
        return None, "FUTURE"
    return dob, None


# --------------------------------------------------------------------------- #
# Gender
# --------------------------------------------------------------------------- #


def standardize_gender(raw: str | None, genders: dict[str, str]) -> str | None:
    """M, F or X through ``genders.csv``; any other value is null."""
    if raw is None:
        return None
    return genders.get(raw.strip(RAW_WHITESPACE).upper())


# --------------------------------------------------------------------------- #
# Whole records
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class StandardRecord:
    """One source record after standardization, with its raw values kept."""

    source_system: str
    source_record_id: str
    first_name_raw: str | None
    last_name_raw: str | None
    dob_raw: str | None
    ssn_raw: str | None
    gender_raw: str | None
    first_name: str | None
    last_name: str | None
    first_name_forms: tuple[str, ...]
    last_name_forms: tuple[str, ...]
    first_name_is_placeholder: bool
    last_name_is_placeholder: bool
    suffix: str | None
    ssn: str | None
    ssn_null_reason: str | None
    dob: date | None
    dob_null_reason: str | None
    dob_is_jan1: bool
    gender: str | None

    @property
    def key(self) -> tuple[str, str]:
        return (self.source_system, self.source_record_id)

    @property
    def has_placeholder_name(self) -> bool:
        return self.first_name_is_placeholder or self.last_name_is_placeholder


def _standard_name(raw, tables: StandardizationTables):
    """Return ``(name, suffix, is_placeholder)`` for one raw name field."""
    name, suffix = split_suffix(clean_name(raw, tables.letters), tables.suffixes)
    if name is not None and name in tables.name_placeholders:
        return None, suffix, True
    return name, suffix, False


def standardize_record(row: dict, tables: StandardizationTables, today: date) -> StandardRecord:
    """Standardize one current record (a row of ``current_records.parquet``).

    ``today`` is the load date (D-026), used to null out future DOBs.
    """
    first, first_suffix, first_placeholder = _standard_name(row.get("first_name"), tables)
    last, last_suffix, last_placeholder = _standard_name(row.get("last_name"), tables)
    ssn, ssn_reason = standardize_ssn(row.get("ssn"), tables.ssn_placeholders)
    dob, dob_reason = standardize_dob(row.get("dob"), tables.dob_placeholders, today)
    return StandardRecord(
        source_system=row["source_system"],
        source_record_id=row["source_record_id"],
        first_name_raw=row.get("first_name"),
        last_name_raw=row.get("last_name"),
        dob_raw=row.get("dob"),
        ssn_raw=row.get("ssn"),
        gender_raw=row.get("gender"),
        first_name=first,
        last_name=last,
        first_name_forms=name_forms(first, tables),
        last_name_forms=name_forms(last, tables),
        first_name_is_placeholder=first_placeholder,
        last_name_is_placeholder=last_placeholder,
        suffix=last_suffix or first_suffix,
        ssn=ssn,
        ssn_null_reason=ssn_reason,
        dob=dob,
        dob_null_reason=dob_reason,
        dob_is_jan1=dob is not None and (dob.month, dob.day) == (1, 1),
        gender=standardize_gender(row.get("gender"), tables.genders),
    )
