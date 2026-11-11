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
from collections.abc import Sequence

from pipeline import config, reference

EXIT_OK = 0
EXIT_NOT_IMPLEMENTED = 2

# Command -> build phase that implements it.
_PLANNED_COMMANDS: dict[str, tuple[int, str]] = {
    "generate": (2, "Generate synthetic CDC loads with ground truth"),
    "ingest": (3, "Apply CDC events to the current-state table"),
    "match-dbt": (5, "Run matching with dbt on DuckDB"),
    "evaluate": (6, "Score results against ground truth"),
    "match-spark": (7, "Run matching with PySpark"),
}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m pipeline",
        description="Person matching pipeline. See docs/DESIGN.md.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("check-config", help="Load and validate all configuration and reference data")

    for name, (phase, help_text) in _PLANNED_COMMANDS.items():
        command = sub.add_parser(name, help=f"{help_text} (phase {phase})")
        if name == "generate":
            command.add_argument("--preset", default=None, help="small | medium | large")
            command.add_argument("--seed", type=int, default=None, help="Override the config seed")
        if name in {"match-dbt", "match-spark", "evaluate"}:
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
        f"{len(ref.suffixes)} suffix forms"
    )
    return EXIT_OK


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

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
