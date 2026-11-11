"""Core data model of the simulation.

A **Person** is a real human with an *identity timeline*: their true name,
DOB, SSN and gender, plus dated changes (marriage, legal name change, a
corrected DOB, a new SSN). Life events are entries on that timeline, so they
compound naturally: someone can marry, then have their DOB corrected.

A **Record** is one source system's copy of a person. It renders the person's
identity as of a given load, then applies the record's own distortions:

* ``overrides`` replace one field's value (a typo, a nickname, a placeholder).
  Each override remembers the identity value it was derived from (``base``)
  and only applies while the person's value is unchanged: after a marriage,
  the source captures the new surname and the old typo no longer applies.
  At most one override exists per field (D-020).
* ``flags`` hold structural rendering choices (middle name in first name,
  suffix written in the last name, names swapped, ...).

Ground truth is the mapping record -> person plus the edge case tags that
apply to the record.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date
from typing import Any

# Identity fields that an override can target.
OVERRIDABLE_FIELDS = ("first", "last", "ssn", "dob", "gender")


@dataclass(slots=True)
class Identity:
    """A person's true identifying attributes at one point in time."""

    first: str | None
    middle: str | None
    last: str | None
    suffix: str | None
    dob: date | None
    ssn: str | None
    gender: str

    def value(self, name: str) -> Any:
        return getattr(self, name)


@dataclass(slots=True)
class IdentityChange:
    """A dated change to a person's identity (one life event).

    ``new_records_only`` changes (SSN_REPLACEMENT) apply to records created
    after the change; existing records keep the old value.
    """

    load_id: int
    code: str
    fields: dict[str, Any]
    new_records_only: bool = False


@dataclass(slots=True)
class Person:
    """A real person in the synthetic population."""

    person_id: str
    culture: str
    identity: Identity
    record_count: int
    is_newborn: bool = False
    is_mononym: bool = False
    # Full compound surname ("García López") and its parts, if any.
    compound_surname: tuple[str, str] | None = None
    # Override of the source's baseline SSN-missing rate (COMMON_NAME_SAME_DOB).
    ssn_missing_rate: float | None = None
    newborn_placeholder: bool = False
    # How the DOB is derived once records are scheduled; see population.py.
    dob_rule: tuple = ("fixed",)
    changes: list[IdentityChange] = field(default_factory=list)
    # Edge case code -> first load from which this person's records carry it.
    tags: dict[str, int] = field(default_factory=dict)
    record_loads: list[int] = field(default_factory=list)
    records: list[Record] = field(default_factory=list)

    def identity_at(self, load_id: int, created_load: int | None = None) -> Identity:
        """The person's identity as of ``load_id``, with all changes up to then applied.

        Pass ``created_load`` when rendering a record: changes marked
        ``new_records_only`` are skipped for records created before them.
        """
        current = self.identity
        for change in self.changes:
            if change.load_id > load_id:
                break
            if (
                change.new_records_only
                and created_load is not None
                and created_load < change.load_id
            ):
                continue
            current = replace(current, **change.fields)
        return current

    def tag(self, code: str, load_id: int) -> None:
        self.tags.setdefault(code, load_id)

    @property
    def first_load(self) -> int:
        return self.record_loads[0]

    @property
    def last_load(self) -> int:
        return self.record_loads[-1]


@dataclass(slots=True)
class Override:
    """Replacement value for one identity field on one record."""

    value: Any
    base: Any
    code: str
    until_load: int | None = None

    def active(self, load_id: int, current_base: Any) -> bool:
        if self.until_load is not None and load_id >= self.until_load:
            return False
        return self.base == current_base


@dataclass(slots=True)
class Record:
    """One source system's record of a person (one record life)."""

    source_system: str
    source_record_id: str
    person: Person
    created_load: int
    overrides: dict[str, Override] = field(default_factory=dict)
    flags: dict[str, Any] = field(default_factory=dict)
    # Edge case code -> load from which it applies to this record.
    tags: dict[str, int] = field(default_factory=dict)
    alive: bool = True
    last_row: dict[str, str | None] | None = None
    last_truth_tags: tuple[str, ...] | None = None

    @property
    def key(self) -> tuple[str, str]:
        return (self.source_system, self.source_record_id)

    def tag(self, code: str, load_id: int) -> None:
        self.tags.setdefault(code, load_id)

    def uses(self, slot: str) -> bool:
        """True if a distortion already occupies this field or rendering slot."""
        return slot in self.overrides or slot in self.flags.get("slots", ())

    def occupy(self, slot: str) -> None:
        self.flags.setdefault("slots", []).append(slot)

    def truth_tags(self, load_id: int) -> tuple[str, ...]:
        """All edge case codes that apply to this record as of ``load_id``."""
        codes = {c for c, start in self.tags.items() if start <= load_id}
        codes.update(c for c, start in self.person.tags.items() if start <= load_id)
        return tuple(sorted(codes))
