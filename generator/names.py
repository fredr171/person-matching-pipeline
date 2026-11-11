"""Frequency-weighted name pools (D-014).

Names live in ``generator/data/`` as CSVs in their natural Unicode form
("José", "De la Cruz", "O'Brien"). Rendering into a source's casing and the
edge-case variations happen later; pools only answer "draw a plausible name".
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from generator.rng import pick

DATA_DIR = Path(__file__).resolve().parent / "data"

# Names ranked this high or better in their culture count as "common" for
# COMMON_NAME_SAME_DOB.
COMMON_NAME_RANK = 5


@dataclass(frozen=True)
class _WeightedPool:
    names: tuple[str, ...]
    probabilities: np.ndarray

    def draw(self, rng: np.random.Generator) -> str:
        return self.names[int(rng.choice(len(self.names), p=self.probabilities))]


def _pool(rows: list[tuple[str, float]]) -> _WeightedPool:
    weights = np.array([w for _, w in rows], dtype=float)
    return _WeightedPool(tuple(n for n, _ in rows), weights / weights.sum())


def _read(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


class NamePools:
    """First names by (culture, gender), last names by culture, sound-alikes."""

    def __init__(self, data_dir: Path = DATA_DIR) -> None:
        first_rows = _read(data_dir / "first_names.csv")
        last_rows = _read(data_dir / "last_names.csv")

        self._first: dict[tuple[str, str], _WeightedPool] = {}
        self._common_first: dict[str, set[str]] = {}
        groups: dict[tuple[str, str], list[tuple[str, float]]] = {}
        for row in first_rows:
            groups.setdefault((row["culture"], row["gender"]), []).append(
                (row["name"], float(row["weight"]))
            )
        for key, rows in groups.items():
            self._first[key] = _pool(rows)
            top = sorted(rows, key=lambda r: (-r[1], r[0]))[:COMMON_NAME_RANK]
            self._common_first.setdefault(key[0], set()).update(n for n, _ in top)

        self._last: dict[str, _WeightedPool] = {}
        self._common_last: dict[str, set[str]] = {}
        last_groups: dict[str, list[tuple[str, float]]] = {}
        for row in last_rows:
            last_groups.setdefault(row["culture"], []).append((row["name"], float(row["weight"])))
        for culture, rows in last_groups.items():
            self._last[culture] = _pool(rows)
            top = sorted(rows, key=lambda r: (-r[1], r[0]))[:COMMON_NAME_RANK]
            self._common_last[culture] = {n for n, _ in top}

        self.phonetic: dict[str, tuple[str, ...]] = {}
        for row in _read(data_dir / "phonetic_variants.csv"):
            key = row["name"].upper()
            self.phonetic[key] = (*self.phonetic.get(key, ()), row["variant"])

        self.cultures: tuple[str, ...] = tuple(sorted(self._last))

    def first_name(self, rng: np.random.Generator, culture: str, gender: str) -> str:
        """Draw a first name. Gender X draws from either gender's names."""
        if gender == "X":
            gender = pick(rng, ("F", "M"))
        return self._first[(culture, gender)].draw(rng)

    def last_name(self, rng: np.random.Generator, culture: str) -> str:
        return self._last[culture].draw(rng)

    def different_last_name(self, rng: np.random.Generator, culture: str, current: str) -> str:
        """Draw a last name different from ``current`` (marriage, adoption)."""
        for _ in range(50):
            name = self.last_name(rng, culture)
            if name != current:
                return name
        raise RuntimeError(f"Could not draw a last name different from {current!r}")

    def different_first_name(
        self, rng: np.random.Generator, culture: str, gender: str, avoid: set[str]
    ) -> str:
        """Draw a first name not in ``avoid`` (twins, legal name changes)."""
        for _ in range(100):
            name = self.first_name(rng, culture, gender)
            if name not in avoid:
                return name
        raise RuntimeError(f"Could not draw a first name avoiding {sorted(avoid)}")

    def common_first_names(self, culture: str) -> frozenset[str]:
        return frozenset(self._common_first.get(culture, set()))

    def common_last_names(self, culture: str) -> frozenset[str]:
        return frozenset(self._common_last.get(culture, set()))
