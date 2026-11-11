"""Entry point: generate all loads for a preset and seed.

The run is fully determined by (config, preset, seed): every random draw
comes from a labelled stream of :mod:`generator.rng`, so the same inputs give
byte-identical files (DESIGN.md section 3.5).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from generator.context import Context
from generator.identifiers import SsnAllocator, add_months
from generator.names import NamePools
from generator.population import build_population
from generator.rng import stream
from generator.simulate import Simulation, plan_refresh_ops
from generator.timeline import schedule_life_events
from generator.writer import LoadWriter
from pipeline import paths, reference
from pipeline.config import GeneratorConfig, load_generator_config


@dataclass(frozen=True)
class GenerationSummary:
    preset: str
    seed: int
    loads: int
    people: int
    records: int
    events: int


def generate(
    config: GeneratorConfig | None = None,
    preset_name: str | None = None,
    seed: int | None = None,
    data_dir: Path = paths.DEFAULT_DATA_DIR,
) -> GenerationSummary:
    """Generate every load and its ground truth into ``data_dir``."""
    config = config or load_generator_config()
    preset = config.preset(preset_name)
    seed = config.seed if seed is None else seed
    ref = reference.load_reference_data()

    load_dates = [add_months(config.simulation_start, i) for i in range(preset.refresh_loads + 1)]
    ctx = Context(
        config=config,
        reference=ref,
        pools=NamePools(),
        ssns=SsnAllocator(stream(seed, "ssn"), ref.placeholder_values("ssn")),
        load_dates=load_dates,
    )

    refresh_ops = plan_refresh_ops(ctx, preset, stream(seed, "load-sizes"))
    inserts = [preset.initial_records] + [ops.inserts for ops in refresh_ops]
    population = build_population(ctx, inserts, stream(seed, "population"))
    schedule_life_events(ctx, population.persons, stream(seed, "timeline"))

    writer = LoadWriter(data_dir)
    writer.reset()
    simulation = Simulation(ctx, refresh_ops, seed)
    events = 0
    for output in simulation.run(population.persons, population.schedule):
        writer.write(output)
        events += len(output.events)

    people = sum(1 for p in population.persons if p.records)
    records = sum(len(p.records) for p in population.persons)
    summary = GenerationSummary(preset.name, seed, len(load_dates), people, records, events)
    writer.write_run_manifest(
        {
            "preset": preset.name,
            "seed": seed,
            "loads": len(load_dates),
            "load_dates": [d.isoformat() for d in load_dates],
        }
    )
    return summary
