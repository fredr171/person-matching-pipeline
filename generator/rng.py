"""Seeded random streams.

Each concern (population, scheduling, record distortions, ...) gets its own
stream derived from the run seed and a fixed label. Adding draws to one
concern therefore never shifts the random numbers another concern sees,
which keeps unrelated parts of the output stable while the generator evolves.

Labels are hashed with CRC32 rather than Python's ``hash()``, which is salted
per process and would break reproducibility.
"""

from __future__ import annotations

import zlib

import numpy as np


def stream(seed: int, label: str) -> np.random.Generator:
    """Return an independent, reproducible generator for one concern."""
    return np.random.default_rng(np.random.SeedSequence([seed, zlib.crc32(label.encode())]))


def chance(rng: np.random.Generator, probability: float) -> bool:
    """True with the given probability."""
    return probability > 0 and rng.random() < probability


def pick(rng: np.random.Generator, items: list | tuple):
    """Choose one item uniformly."""
    return items[int(rng.integers(len(items)))]
