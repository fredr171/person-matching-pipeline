"""Render ``docs/DATA_GENERATION.md`` from ``config/generator.yaml``.

DESIGN.md section 3.7 requires the catalog documentation to be generated
from the config so the two cannot drift. Regenerate with
``python -m pipeline docs``; a test fails if the committed file is stale.
"""

from __future__ import annotations

from pathlib import Path

from pipeline import paths
from pipeline.config import GeneratorConfig, load_generator_config

DOC_PATH = paths.REPO_ROOT / "docs" / "DATA_GENERATION.md"

CATEGORY_TITLES = {
    "identifier": "Identifier errors and placeholders",
    "name": "Name variation",
    "life_event": "Life and legal events",
    "family": "Family and look-alike cases",
    "fraud": "Fraud",
    "source_pipeline": "Source system and pipeline",
}


def _percent(rate: float) -> str:
    return f"{rate * 100:g}%"


def _cell(text: str) -> str:
    return text.replace("|", "\\|")


def render_markdown(config: GeneratorConfig) -> str:
    lines: list[str] = []
    add = lines.append

    add("# Data Generation")
    add("")
    add("<!-- Generated from config/generator.yaml by `python -m pipeline docs`. Do not edit. -->")
    add("")
    add(
        "The generator builds a synthetic population of true people, then emits their "
        "records from several source systems as monthly CDC loads. Ground truth "
        "(which record belongs to which person, and which edge cases apply) is written "
        "to a separate directory that only `evaluation/` and the tests may read."
    )
    add("")
    add(
        f"Default seed: `{config.seed}`. Load 0 is dated {config.simulation_start.isoformat()} "
        "and each refresh load is one month later. The same config, preset and seed always "
        "produce byte-identical files."
    )
    add("")

    add("## Presets")
    add("")
    add("| Preset | Initial records | Records per refresh | Refresh loads |")
    add("|---|---:|---:|---:|")
    for preset in config.presets.values():
        default = " (default)" if preset.name == config.default_preset else ""
        add(
            f"| {preset.name}{default} | {preset.initial_records:,} | "
            f"{preset.refresh_records_min:,}–{preset.refresh_records_max:,} | "
            f"{preset.refresh_loads} |"
        )
    add("")
    mix = config.refresh_mix
    add(
        f"Each refresh is split {_percent(mix.insert)} inserts, {_percent(mix.update)} updates "
        f"and {_percent(mix.delete)} deletes. Edge cases add events on top: corrections, "
        "migrations, duplicates and replays."
    )
    add("")

    add("## Population")
    add("")
    rpp = config.records_per_person
    counts = ", ".join(f"{n}: {_percent(p)}" for n, p in rpp.counts.items())
    add(
        f"- **Records per person:** {counts}, {rpp.tail_min}–{rpp.tail_max}: "
        f"{_percent(rpp.tail_probability)} (expected mean {rpp.expected_mean():.3f})."
    )
    pop = config.population
    cultures = ", ".join(f"{k.replace('_', ' ')} {_percent(v)}" for k, v in pop.culture_mix.items())
    add(f"- **Name cultures:** {cultures}. Names come from `generator/data/`.")
    add(
        f"- **Ages:** {pop.min_age_years}–{pop.max_age_years} years, plus "
        f"{_percent(pop.newborn_share)} newborns. {_percent(pop.leap_day_birth_share)} are "
        "born on Feb 29."
    )
    add(
        f"- **Names:** {_percent(pop.middle_name_rate)} have a middle name; "
        f"{_percent(pop.compound_surname_rate)} of Hispanic people have a two-part surname; "
        f"{_percent(pop.suffix_rate)} of men have a generational suffix."
    )
    add("")

    add("## Source systems")
    add("")
    add(
        "Formatting differences between sources are baseline noise, not edge cases. "
        "Standardization must handle all of them."
    )
    add("")
    add("| Source | Share | Names | DOB | SSN | SSN missing | Gender missing |")
    add("|---|---:|---|---|---|---:|---:|")
    for s in config.source_systems:
        name = (
            f"{s.name} (family name first for East Asian names)" if s.family_name_first else s.name
        )
        add(
            f"| {name} | {_percent(s.weight)} | {s.name_case} | {s.dob_format} | "
            f"{s.ssn_format} | {_percent(s.ssn_missing_rate)} | {_percent(s.gender_missing_rate)} |"
        )
    add("")
    add(
        "DOB formats: iso `YYYY-MM-DD`, us `MM/DD/YYYY`, compact `YYYYMMDD`. "
        "SSN formats: dashed `123-45-6789`, digits `123456789`, spaced `123 45 6789`. "
        "After a migration a source rotates to the next format in each list."
    )
    add("")

    add("## Edge case catalog")
    add("")
    add(
        "Rates are **deliberately exaggerated** (D-008) so every case is frequent enough "
        "to measure. A rate is the probability that an *eligible* unit gets the case "
        "(D-016). The unit is a person, a record, an event or a whole load."
    )
    add("")
    add(
        "A field carries at most one distortion (D-020), so an SSN never has both a typo "
        "and a placeholder. Distortions of different fields stack, and life events stack "
        "on the same person."
    )
    add("")
    for category, title in CATEGORY_TITLES.items():
        cases = [c for c in config.edge_cases if c.category == category]
        add(f"### {title}")
        add("")
        add("| Code | Description | How it is generated | Expected outcome | Unit | Rate |")
        add("|---|---|---|---|---|---:|")
        for c in cases:
            add(
                f"| `{c.code}` | {_cell(c.description)} | {_cell(c.generation)} | "
                f"{_cell(c.expected_outcome)} | {c.unit} | {_percent(c.rate)} |"
            )
        add("")

    add("## Caveats")
    add("")
    add(
        "- **Nicknames and transliterations come from the matcher's own reference table** "
        "(`config/reference/nicknames.csv`). Generated variants are therefore always "
        "known to the matcher. Real data contains variants no table lists, so measured "
        "recall on `NAME_NICKNAME` and `NAME_TRANSLITERATION` is an upper bound (D-021)."
    )
    add(
        "- **Phonetic variants** (`generator/data/phonetic_variants.csv`) are checked by "
        "a test to share a Double Metaphone code, so each one tests the phonetic rule "
        "rather than fuzzy matching."
    )
    add(
        "- **SSNs are valid in format only**: random numbers, not linked to any real "
        "person. Generated data is never committed."
    )
    add("")
    return "\n".join(lines)


def write_docs(path: Path = DOC_PATH, config: GeneratorConfig | None = None) -> Path:
    text = render_markdown(config or load_generator_config())
    path.write_text(text, encoding="utf-8", newline="\n")
    return path
