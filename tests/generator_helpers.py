"""Helpers shared by the generator tests."""

from __future__ import annotations

import dataclasses
import hashlib
from pathlib import Path

import pyarrow.parquet as pq

from generator.context import Context
from generator.identifiers import SsnAllocator, add_months
from generator.names import NamePools
from generator.rng import stream
from pipeline import reference
from pipeline.config import GeneratorConfig

# Rates used to make every edge case occur on the small preset.
BOOSTED_RATES = {"record": 0.3, "person": 0.5, "event": 0.3, "load": 1.0}


def boosted(config: GeneratorConfig) -> GeneratorConfig:
    """A copy of the config with high rates, so rare cases show up in small runs."""
    cases = tuple(
        dataclasses.replace(c, rate=max(c.rate, BOOSTED_RATES[c.unit])) for c in config.edge_cases
    )
    return dataclasses.replace(config, edge_cases=cases)


def make_context(config: GeneratorConfig, loads: int = 13, seed: int = 7) -> Context:
    ref = reference.load_reference_data()
    return Context(
        config=config,
        reference=ref,
        pools=NamePools(),
        ssns=SsnAllocator(stream(seed, "ssn"), ref.placeholder_values("ssn")),
        load_dates=[add_months(config.simulation_start, i) for i in range(loads)],
    )


def read_loads(data_dir: Path, tree: str, filename: str) -> dict[int, list[dict]]:
    """Rows of every load's events or truth file, keyed by load id."""
    result = {}
    for path in sorted((data_dir / tree).glob(f"load_*/{filename}")):
        result[int(path.parent.name.split("_")[1])] = pq.read_table(path).to_pylist()
    return result


def file_hashes(data_dir: Path) -> dict[str, str]:
    return {
        path.relative_to(data_dir).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(data_dir.rglob("*"))
        if path.is_file()
    }
