"""Shared state handed to every edge case implementation."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date

from generator.identifiers import SsnAllocator
from generator.model import Record
from generator.names import NamePools
from generator.render import strip_accents
from pipeline.config import GeneratorConfig
from pipeline.reference import ReferenceData


def name_key(name: str) -> str:
    """Comparison key for reference lookups: accents stripped, uppercase."""
    return strip_accents(name).upper()


@dataclass
class Context:
    """Configuration, reference data and services for one generation run."""

    config: GeneratorConfig
    reference: ReferenceData
    pools: NamePools
    ssns: SsnAllocator
    load_dates: list[date]
    # Set by the simulation: request an update event for a record at a load.
    schedule_update: Callable[[Record, int], None] = field(default=lambda record, load: None)

    def __post_init__(self) -> None:
        self.rates = {case.code: case.rate for case in self.config.edge_cases}
        self.nicknames: dict[str, list[str]] = {}
        groups: dict[str, list[str]] = {}
        for row in self.reference.name_variants:
            if row.relation == "nickname":
                self.nicknames.setdefault(row.canonical, []).append(row.variant)
            elif row.relation == "transliteration":
                groups.setdefault(row.canonical, [row.canonical]).append(row.variant)
        # Each member of a transliteration group maps to the other members.
        self.transliterations: dict[str, list[str]] = {}
        for members in groups.values():
            for member in members:
                others = [m for m in members if m != member]
                self.transliterations.setdefault(member, []).extend(others)
        self.name_placeholders = self.reference.placeholder_values("name")
        self.dob_placeholders = sorted(
            date.fromisoformat(v) for v in self.reference.placeholder_values("dob")
        )
        self.ssn_placeholders = sorted(self.reference.placeholder_values("ssn"))

    @property
    def last_load(self) -> int:
        return len(self.load_dates) - 1

    def rate(self, code: str) -> float:
        return self.rates[code]
