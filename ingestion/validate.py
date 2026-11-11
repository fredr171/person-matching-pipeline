"""Step 1 of ingestion: validate each event against the schema contract.

Contract (DESIGN.md section 3.2): every event has an ``event_id``, a key
(``source_system``, ``source_record_id``), a ``sequence_no`` and an ``op`` of
I, U or D. I and U carry the full row image; D carries only keys.

Validation rejects events whose *structure* is broken. It does not judge
values: a placeholder SSN or an unparseable DOB is a data quality problem for
standardization, not a corrupt event. A reject is logged with a reason code
and never retried or queued for review (D-001).

Reason codes, checked in this order:

``REJECT_MISSING_KEY``
    ``event_id``, ``source_system``, ``source_record_id`` or ``sequence_no``
    is missing, so the event cannot be deduplicated, ordered or applied.
``REJECT_BAD_OP``
    ``op`` is not I, U or D.
``REJECT_COLUMN_SHIFT``
    Values sit one column to the right of where they belong (D-028). The
    fingerprint is a DOB containing letters (a name landed in it) plus at
    least one corroborating sign: an empty first name, an SSN that looks
    like a date, or a gender that looks like an identifier.
``REJECT_ENCODING``
    A name is mojibake (UTF-8 bytes decoded as Latin-1, e.g. "JosÃ©") or
    contains control or replacement characters.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

VALID_OPS = frozenset({"I", "U", "D"})
KEY_COLUMNS = ("event_id", "source_system", "source_record_id", "sequence_no")
NAME_COLUMNS = ("first_name", "last_name")

# A UTF-8 lead byte (0xC2-0xDF) followed by a continuation byte (0x80-0xBF),
# both decoded as Latin-1. No legitimate name contains this pair: code points
# U+0080-U+00BF are control characters and symbols, never letters.
_MOJIBAKE = re.compile("[Â-ß][\u0080-¿]")
# C0/C1 control characters and the Unicode replacement character.
_BAD_CHARACTERS = re.compile("[\u0000-\u001f\u007f-\u009f�]")
_DATE_LIKE = re.compile(r"^\s*(\d{4}-\d{1,2}-\d{1,2}|\d{1,2}/\d{1,2}/\d{2,4})\s*$")


@dataclass(frozen=True)
class Rejection:
    reason: str
    detail: str


def _missing(value) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def looks_like_column_shift(event: dict) -> str | None:
    """Describe the column-shift fingerprint if the event has it, else None."""
    dob = event.get("dob") or ""
    if not any(c.isalpha() for c in dob):
        return None
    corroboration = []
    if _missing(event.get("first_name")):
        corroboration.append("first_name empty")
    if _DATE_LIKE.match(event.get("ssn") or ""):
        corroboration.append("ssn looks like a date")
    gender = event.get("gender") or ""
    if len(gender) > 3 and any(c.isdigit() for c in gender):
        corroboration.append("gender looks like an identifier")
    if not corroboration:
        return None
    return "dob contains letters; " + ", ".join(corroboration)


def encoding_problem(value: str | None) -> str | None:
    """Describe an encoding problem in a name value, else None."""
    if not value:
        return None
    if _BAD_CHARACTERS.search(value):
        return "control or replacement character"
    if _MOJIBAKE.search(value):
        return "mojibake (UTF-8 decoded as Latin-1)"
    return None


def validate_event(event: dict) -> Rejection | None:
    """Return why the event violates the contract, or None if it is valid."""
    missing = [c for c in KEY_COLUMNS if _missing(event.get(c))]
    if missing:
        return Rejection("REJECT_MISSING_KEY", "missing " + ", ".join(missing))

    op = event.get("op")
    if op not in VALID_OPS:
        return Rejection("REJECT_BAD_OP", f"op={op!r}")

    if op == "D":
        return None  # deletes carry only keys; nothing else to check

    shift = looks_like_column_shift(event)
    if shift:
        return Rejection("REJECT_COLUMN_SHIFT", shift)

    for column in NAME_COLUMNS:
        problem = encoding_problem(event.get(column))
        if problem:
            return Rejection("REJECT_ENCODING", f"{column}: {problem}")
    return None
