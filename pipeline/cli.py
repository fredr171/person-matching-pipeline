"""Command-line entry point: ``python -m pipeline <command>``.

The Makefile targets are thin wrappers around these commands (D-010), so the
same workflow runs on Windows without ``make``.

Stages that are not built yet exit with status 2 and name the build phase
(DESIGN.md section 17) that will implement them, rather than silently doing
nothing.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Sequence
from pathlib import Path

from pipeline import config, paths, reference

EXIT_OK = 0
EXIT_NOT_IMPLEMENTED = 2

# Command -> build phase that implements it.
_PLANNED_COMMANDS: dict[str, tuple[int, str]] = {
    "match-spark": (7, "Run matching with PySpark"),
}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m pipeline",
        description="Person matching pipeline. See docs/DESIGN.md.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("check-config", help="Load and validate all configuration and reference data")

    generate = sub.add_parser("generate", help="Generate synthetic CDC loads with ground truth")
    generate.add_argument("--preset", default=None, help="small | medium | large")
    generate.add_argument("--seed", type=int, default=None, help="Override the config seed")
    generate.add_argument(
        "--data-dir", type=Path, default=paths.DEFAULT_DATA_DIR, help="Output directory"
    )

    sub.add_parser("docs", help="Regenerate docs/DATA_GENERATION.md from the config")

    ingest = sub.add_parser("ingest", help="Apply CDC events to the current-state table")
    ingest.add_argument(
        "--data-dir", type=Path, default=paths.DEFAULT_DATA_DIR, help="Data directory"
    )
    ingest.add_argument(
        "--through-load", type=int, default=None, help="Stop after this load (default: all)"
    )

    match_dbt = sub.add_parser("match-dbt", help="Run matching with dbt on DuckDB")
    match_dbt.add_argument(
        "--profile", default=None, help="conservative | balanced | aggressive (default: config)"
    )
    match_dbt.add_argument(
        "--data-dir", type=Path, default=paths.DEFAULT_DATA_DIR, help="Data directory"
    )
    match_dbt.add_argument(
        "--through-load", type=int, default=None, help="Stop after this load (default: all)"
    )

    evaluate = sub.add_parser("evaluate", help="Score matched loads against ground truth")
    evaluate.add_argument(
        "--profile", default=None, help="conservative | balanced | aggressive (default: config)"
    )
    evaluate.add_argument("--engine", default="dbt", choices=["dbt", "spark"], help="Engine")
    evaluate.add_argument(
        "--data-dir", type=Path, default=paths.DEFAULT_DATA_DIR, help="Data directory"
    )
    evaluate.add_argument(
        "--through-load", type=int, default=None, help="Stop after this load (default: all)"
    )

    results = sub.add_parser(
        "results", help="Render docs/RESULTS.md from the evaluations of every profile"
    )
    results.add_argument("--engine", default="dbt", choices=["dbt", "spark"], help="Engine")
    results.add_argument(
        "--data-dir", type=Path, default=paths.DEFAULT_DATA_DIR, help="Data directory"
    )

    for name, (phase, help_text) in _PLANNED_COMMANDS.items():
        command = sub.add_parser(name, help=f"{help_text} (phase {phase})")
        if name == "match-spark":
            command.add_argument(
                "--profile", default=None, help="conservative | balanced | aggressive"
            )
    return parser


def _check_config() -> int:
    """Validate every config and reference file and print a short summary."""
    gen = config.load_generator_config()
    match = config.load_matching_config()
    ref = reference.load_reference_data()

    preset = gen.preset()
    print(
        f"generator.yaml  OK  seed={gen.seed}, default preset={preset.name}, "
        f"{len(gen.edge_cases)} edge cases, "
        f"expected records/person={gen.records_per_person.expected_mean():.3f}"
    )
    print(
        f"matching.yaml   OK  default profile={match.default_profile}, "
        f"profiles={sorted(match.profiles)}, blocking keys={len(match.blocking_keys)}"
    )
    print(
        f"reference/      OK  {len(ref.name_variants)} name variants, "
        f"{len(ref.placeholders)} placeholders, {len(ref.particles)} particles, "
        f"{len(ref.suffixes)} suffix forms, {len(ref.letters)} letters, "
        f"{len(ref.genders)} gender values"
    )
    return EXIT_OK


def _generate(preset: str | None, seed: int | None, data_dir: Path) -> int:
    from generator.generate import generate

    started = time.perf_counter()
    summary = generate(preset_name=preset, seed=seed, data_dir=data_dir)
    print(
        f"Generated preset={summary.preset} seed={summary.seed}: {summary.loads} loads, "
        f"{summary.people:,} people, {summary.records:,} records, {summary.events:,} events "
        f"in {time.perf_counter() - started:.1f}s -> {data_dir}"
    )
    return EXIT_OK


def _ingest(data_dir: Path, through_load: int | None) -> int:
    from ingestion.run import ingest_all

    started = time.perf_counter()
    results = ingest_all(data_dir, through_load)
    for m in results:
        rejected = sum(m.rejected.values())
        print(
            f"load {m.load_id:>2}: {m.events_read:>8,} events, {rejected:>5,} rejected, "
            f"{m.duplicates_dropped:>5,} duplicates, {m.stale_ignored:>6,} stale, "
            f"{m.live_records:>9,} live records"
        )
    print(f"Ingested {len(results)} loads in {time.perf_counter() - started:.1f}s")
    return EXIT_OK


def _match_dbt(profile: str | None, data_dir: Path, through_load: int | None) -> int:
    from pipeline.match_dbt import match_all

    started = time.perf_counter()
    count = 0
    for run in match_all(data_dir, profile, through_load):
        m = run.metrics
        count += 1
        print(
            f"load {run.load_id:>2}: {m.get(('records', 'live_records'), 0):>9,} records, "
            f"{m.get(('pairs', 'compared'), 0):>9,} pairs, "
            f"{m.get(('pairs', 'matched'), 0):>8,} matches, "
            f"{m.get(('clustering', 'clusters'), 0):>9,} clusters, "
            f"{m.get(('person_ids', 'records_with_changed_id'), 0):>6,} id changes "
            f"({run.seconds:.1f}s)"
        )
    print(f"Matched {count} loads with dbt in {time.perf_counter() - started:.1f}s")
    return EXIT_OK


def _evaluate(profile: str | None, engine: str, data_dir: Path, through_load: int | None) -> int:
    from evaluation.run import evaluate_all

    name = config.load_matching_config().profile(profile).name
    for e in evaluate_all(engine, name, data_dir, through_load):
        p, churn = e.pairwise, e.instability
        print(
            f"load {e.load_id:>2}: precision {p.precision or 0:.4f}, recall {p.recall or 0:.4f}, "
            f"F1 {p.f1 or 0:.4f}, blocking recall {e.stages.blocking_recall or 0:.4f}, "
            f"false-merge records {e.outcome_counts.get('FALSE_MERGE', 0):,}"
            + (f", ID changes {churn.changed:,}" if churn else "")
        )
    print(f"Wrote {paths.evaluation_dir(engine, name, 0, data_dir).parent}")
    return EXIT_OK


def _results(engine: str, data_dir: Path) -> int:
    from evaluation.report import write_results

    print(f"Wrote {write_results(data_dir, engine)}")
    return EXIT_OK


def _docs() -> int:
    from generator.docs import write_docs

    print(f"Wrote {write_docs()}")
    return EXIT_OK


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    if args.command == "generate":
        return _generate(args.preset, args.seed, args.data_dir)
    if args.command == "docs":
        return _docs()
    if args.command == "ingest":
        return _ingest(args.data_dir, args.through_load)
    if args.command == "match-dbt":
        return _match_dbt(args.profile, args.data_dir, args.through_load)
    if args.command == "evaluate":
        return _evaluate(args.profile, args.engine, args.data_dir, args.through_load)
    if args.command == "results":
        return _results(args.engine, args.data_dir)

    if args.command == "check-config":
        try:
            return _check_config()
        except (config.ConfigError, reference.ReferenceDataError) as error:
            print(f"Configuration error: {error}", file=sys.stderr)
            return 1

    phase, _ = _PLANNED_COMMANDS[args.command]
    print(
        f"'{args.command}' is not implemented yet; it arrives in build phase {phase} "
        f"(docs/DESIGN.md section 17).",
        file=sys.stderr,
    )
    return EXIT_NOT_IMPLEMENTED
