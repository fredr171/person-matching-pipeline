"""Typed, validated access to ``config/generator.yaml`` and ``config/matching.yaml``.

The YAML files are the single source of truth for parameters. This module turns
them into frozen dataclasses and rejects invalid values at load time, so a bad
config fails immediately with a clear message instead of producing subtly
wrong data or matches several stages later.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import yaml

from pipeline import paths

# Rates are probabilities that an eligible unit receives an edge case (D-016).
EDGE_CASE_UNITS: frozenset[str] = frozenset({"person", "record", "event", "load"})
EDGE_CASE_CATEGORIES: frozenset[str] = frozenset(
    {"identifier", "name", "life_event", "family", "fraud", "source_pipeline"}
)
NAME_CASES: frozenset[str] = frozenset({"title", "upper", "mixed"})
DOB_FORMATS: tuple[str, ...] = ("iso", "us", "compact")
SSN_FORMATS: tuple[str, ...] = ("dashed", "digits", "spaced")
GENDERS: frozenset[str] = frozenset({"M", "F", "X"})
CULTURES: frozenset[str] = frozenset(
    {"anglo", "hispanic", "east_asian", "south_asian", "arabic", "european"}
)
# Blocking keys from DESIGN.md section 6. Each implementation maps these names
# to its own expression; an unknown name is a config error, not a silent no-op.
BLOCKING_KEYS: frozenset[str] = frozenset(
    {
        "ssn",
        "dob",
        "ssn_sorted_digits_birth_year",
        "ssn_first5_birth_year",
        "last_phonetic_first_initial_birth_year",
    }
)

_PROBABILITY_TOLERANCE = 1e-9


class ConfigError(ValueError):
    """Raised when a configuration file is missing values or contains invalid ones."""


# --------------------------------------------------------------------------- #
# Generator config
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Preset:
    """Dataset size preset (DESIGN.md section 3.4)."""

    name: str
    initial_records: int
    refresh_records_min: int
    refresh_records_max: int
    refresh_loads: int


@dataclass(frozen=True)
class RefreshMix:
    """Share of inserts, updates and deletes in each refresh load."""

    insert: float
    update: float
    delete: float


@dataclass(frozen=True)
class RecordsPerPerson:
    """Distribution of how many source records each true person has.

    ``counts`` maps an exact record count to its probability. ``tail_probability``
    is the chance of drawing uniformly from ``tail_min..tail_max`` instead.
    """

    counts: dict[int, float]
    tail_probability: float
    tail_min: int
    tail_max: int

    def expected_mean(self) -> float:
        """Expected records per person; DESIGN.md section 3.5 targets about 1.4."""
        tail_mean = (self.tail_min + self.tail_max) / 2
        return sum(n * p for n, p in self.counts.items()) + self.tail_probability * tail_mean


@dataclass(frozen=True)
class Population:
    """Shape of the generated population (ages, genders, name cultures)."""

    min_age_years: int
    max_age_years: int
    newborn_share: float
    leap_day_birth_share: float
    gender_mix: dict[str, float]
    culture_mix: dict[str, float]
    middle_name_rate: float
    compound_surname_rate: float
    suffix_rate: float


@dataclass(frozen=True)
class SourceSystem:
    """A simulated source system feeding CDC events, with its formatting habits."""

    name: str
    weight: float
    family_name_first: bool
    name_case: str
    dob_format: str
    ssn_format: str
    ssn_missing_rate: float
    gender_unknown_value: str
    gender_missing_rate: float


@dataclass(frozen=True)
class Migration:
    """What a source looks like after MIGRATION_REFORMAT."""

    name_case: str
    first_name_max_length: int
    last_name_max_length: int
    default_ssn: str
    default_gender: str


@dataclass(frozen=True)
class PipelineCases:
    """Parameters of the source-system and pipeline edge cases."""

    migration: Migration
    source_id_change_share: float
    column_shift_batch_share: float


@dataclass(frozen=True)
class EdgeCase:
    """One entry of the edge case catalog (DESIGN.md section 3.7)."""

    code: str
    category: str
    description: str
    generation: str
    expected_outcome: str
    unit: str
    rate: float


@dataclass(frozen=True)
class GeneratorConfig:
    """Everything the synthetic data generator is parameterised by."""

    seed: int
    default_preset: str
    simulation_start: date
    presets: dict[str, Preset]
    refresh_mix: RefreshMix
    records_per_person: RecordsPerPerson
    population: Population
    source_systems: tuple[SourceSystem, ...]
    pipeline_cases: PipelineCases
    edge_cases: tuple[EdgeCase, ...]

    def source(self, name: str) -> SourceSystem:
        """Return the named source system."""
        for source in self.source_systems:
            if source.name == name:
                return source
        raise ConfigError(f"Unknown source system {name!r}")

    def preset(self, name: str | None = None) -> Preset:
        """Return the named preset, or the default preset when ``name`` is None."""
        key = name or self.default_preset
        try:
            return self.presets[key]
        except KeyError:
            raise ConfigError(
                f"Unknown preset {key!r}; expected one of {sorted(self.presets)}"
            ) from None

    def edge_case(self, code: str) -> EdgeCase:
        """Return the catalog entry for an edge case code."""
        for case in self.edge_cases:
            if case.code == code:
                return case
        raise ConfigError(f"Unknown edge case code {code!r}")


# --------------------------------------------------------------------------- #
# Matching config
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Profile:
    """A matching profile (DESIGN.md section 12).

    A Jaro-Winkler score ``s`` is STRONG when ``s >= strong_name_threshold`` and
    WEAK when ``weak_name_threshold <= s < strong_name_threshold``.
    """

    name: str
    strong_name_threshold: float
    weak_name_threshold: float
    tier_4_matches: bool
    use_case: str


@dataclass(frozen=True)
class PersonIdFormat:
    """Format of stable person IDs, e.g. ``P000000123`` (DESIGN.md section 11)."""

    prefix: str
    digits: int

    def format(self, sequence_number: int) -> str:
        """Render a sequence number as a person ID."""
        if sequence_number < 1:
            raise ValueError("Person ID sequence numbers start at 1")
        if sequence_number >= 10**self.digits:
            raise ValueError(f"Sequence number {sequence_number} exceeds {self.digits} digits")
        return f"{self.prefix}{sequence_number:0{self.digits}d}"


@dataclass(frozen=True)
class MatchingConfig:
    """Thresholds, blocker parameters and blocking settings shared by both engines."""

    default_profile: str
    profiles: dict[str, Profile]
    name_prefix_min_length: int
    generational_gap_years: int
    newborn_max_age_years: int
    max_block_size: int
    blocking_keys: tuple[str, ...]
    person_id: PersonIdFormat

    def profile(self, name: str | None = None) -> Profile:
        """Return the named profile, or the default profile when ``name`` is None."""
        key = name or self.default_profile
        try:
            return self.profiles[key]
        except KeyError:
            raise ConfigError(
                f"Unknown profile {key!r}; expected one of {sorted(self.profiles)}"
            ) from None


# --------------------------------------------------------------------------- #
# Loading and validation
# --------------------------------------------------------------------------- #


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ConfigError(f"Config file not found: {path}")
    with path.open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ConfigError(f"{path} must contain a YAML mapping at the top level")
    return data


def _require(mapping: dict[str, Any], key: str, where: str) -> Any:
    if key not in mapping or mapping[key] is None:
        raise ConfigError(f"Missing required key {key!r} in {where}")
    return mapping[key]


def _check_probability(value: float, where: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not 0 <= value <= 1:
        raise ConfigError(f"{where} must be a number between 0 and 1, got {value!r}")
    return float(value)


def _check_positive_int(value: Any, where: str, allow_zero: bool = False) -> int:
    minimum = 0 if allow_zero else 1
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise ConfigError(f"{where} must be an integer >= {minimum}, got {value!r}")
    return value


def _check_sums_to_one(values: list[float], where: str) -> None:
    total = sum(values)
    if abs(total - 1.0) > _PROBABILITY_TOLERANCE:
        raise ConfigError(f"{where} must sum to 1, got {total:.6f}")


def load_generator_config(path: Path = paths.GENERATOR_CONFIG) -> GeneratorConfig:
    """Load and validate the generator configuration."""
    raw = _read_yaml(path)

    presets: dict[str, Preset] = {}
    for name, spec in _require(raw, "presets", "generator config").items():
        where = f"presets.{name}"
        refresh = _require(spec, "refresh_records", where)
        preset = Preset(
            name=name,
            initial_records=_check_positive_int(
                _require(spec, "initial_records", where), f"{where}.initial_records"
            ),
            refresh_records_min=_check_positive_int(
                _require(refresh, "min", f"{where}.refresh_records"),
                f"{where}.refresh_records.min",
            ),
            refresh_records_max=_check_positive_int(
                _require(refresh, "max", f"{where}.refresh_records"),
                f"{where}.refresh_records.max",
            ),
            refresh_loads=_check_positive_int(
                _require(spec, "refresh_loads", where), f"{where}.refresh_loads", allow_zero=True
            ),
        )
        if preset.refresh_records_min > preset.refresh_records_max:
            raise ConfigError(f"{where}.refresh_records.min must not exceed max")
        presets[name] = preset

    default_preset = _require(raw, "default_preset", "generator config")
    if default_preset not in presets:
        raise ConfigError(f"default_preset {default_preset!r} is not a defined preset")

    mix_raw = _require(raw, "refresh_mix", "generator config")
    refresh_mix = RefreshMix(
        insert=_check_probability(_require(mix_raw, "insert", "refresh_mix"), "refresh_mix.insert"),
        update=_check_probability(_require(mix_raw, "update", "refresh_mix"), "refresh_mix.update"),
        delete=_check_probability(_require(mix_raw, "delete", "refresh_mix"), "refresh_mix.delete"),
    )
    _check_sums_to_one([refresh_mix.insert, refresh_mix.update, refresh_mix.delete], "refresh_mix")

    rpp_raw = dict(_require(raw, "records_per_person", "generator config"))
    tail_probability = _check_probability(rpp_raw.pop("tail", 0.0), "records_per_person.tail")
    tail_min = _check_positive_int(rpp_raw.pop("tail_min", 1), "records_per_person.tail_min")
    tail_max = _check_positive_int(rpp_raw.pop("tail_max", 1), "records_per_person.tail_max")
    counts: dict[int, float] = {}
    for key, probability in rpp_raw.items():
        try:
            count = int(key)
        except (TypeError, ValueError):
            raise ConfigError(f"records_per_person key {key!r} must be an integer") from None
        counts[_check_positive_int(count, "records_per_person key")] = _check_probability(
            probability, f"records_per_person.{key}"
        )
    if tail_min > tail_max:
        raise ConfigError("records_per_person.tail_min must not exceed tail_max")
    if counts and tail_probability and tail_min <= max(counts):
        raise ConfigError("records_per_person.tail_min must exceed the largest explicit count")
    _check_sums_to_one([*counts.values(), tail_probability], "records_per_person")
    records_per_person = RecordsPerPerson(counts, tail_probability, tail_min, tail_max)

    population = _parse_population(_require(raw, "population", "generator config"))

    sources = tuple(_parse_source(s) for s in _require(raw, "source_systems", "generator config"))
    if len({s.name for s in sources}) != len(sources):
        raise ConfigError("source_systems names must be unique")
    _check_sums_to_one([s.weight for s in sources], "source_systems weights")

    pipeline_cases = _parse_pipeline_cases(_require(raw, "pipeline_cases", "generator config"))

    simulation_start = _require(raw, "simulation_start", "generator config")
    if not isinstance(simulation_start, date):
        raise ConfigError("simulation_start must be a date (YYYY-MM-DD)")

    edge_cases = tuple(_parse_edge_case(e) for e in _require(raw, "edge_cases", "generator config"))
    codes = [e.code for e in edge_cases]
    duplicates = sorted({c for c in codes if codes.count(c) > 1})
    if duplicates:
        raise ConfigError(f"Duplicate edge case codes: {duplicates}")

    seed = _check_positive_int(_require(raw, "seed", "generator config"), "seed", allow_zero=True)

    return GeneratorConfig(
        seed=seed,
        default_preset=default_preset,
        simulation_start=simulation_start,
        presets=presets,
        refresh_mix=refresh_mix,
        records_per_person=records_per_person,
        population=population,
        source_systems=sources,
        pipeline_cases=pipeline_cases,
        edge_cases=edge_cases,
    )


def _parse_mix(raw: Any, allowed: frozenset[str], where: str) -> dict[str, float]:
    if not isinstance(raw, dict) or not raw:
        raise ConfigError(f"{where} must be a non-empty mapping")
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise ConfigError(f"{where}: unknown keys {unknown}; expected from {sorted(allowed)}")
    mix = {key: _check_probability(value, f"{where}.{key}") for key, value in raw.items()}
    _check_sums_to_one(list(mix.values()), where)
    return mix


def _parse_population(raw: dict[str, Any]) -> Population:
    def probability(key: str) -> float:
        return _check_probability(_require(raw, key, "population"), f"population.{key}")

    population = Population(
        min_age_years=_check_positive_int(
            _require(raw, "min_age_years", "population"), "population.min_age_years", True
        ),
        max_age_years=_check_positive_int(
            _require(raw, "max_age_years", "population"), "population.max_age_years"
        ),
        newborn_share=probability("newborn_share"),
        leap_day_birth_share=probability("leap_day_birth_share"),
        gender_mix=_parse_mix(_require(raw, "gender_mix", "population"), GENDERS, "gender_mix"),
        culture_mix=_parse_mix(_require(raw, "culture_mix", "population"), CULTURES, "culture_mix"),
        middle_name_rate=probability("middle_name_rate"),
        compound_surname_rate=probability("compound_surname_rate"),
        suffix_rate=probability("suffix_rate"),
    )
    if population.min_age_years >= population.max_age_years:
        raise ConfigError("population.min_age_years must be below max_age_years")
    return population


def _parse_choice(value: Any, allowed: Any, where: str) -> str:
    if value not in allowed:
        raise ConfigError(f"{where} must be one of {sorted(allowed)}, got {value!r}")
    return value


def _parse_source(raw: dict[str, Any]) -> SourceSystem:
    name = _require(raw, "name", "source_systems entry")
    where = f"source_systems.{name}"
    return SourceSystem(
        name=name,
        weight=_check_probability(_require(raw, "weight", where), f"{where}.weight"),
        family_name_first=bool(raw.get("family_name_first", False)),
        name_case=_parse_choice(_require(raw, "name_case", where), NAME_CASES, where),
        dob_format=_parse_choice(_require(raw, "dob_format", where), DOB_FORMATS, where),
        ssn_format=_parse_choice(_require(raw, "ssn_format", where), SSN_FORMATS, where),
        ssn_missing_rate=_check_probability(
            _require(raw, "ssn_missing_rate", where), f"{where}.ssn_missing_rate"
        ),
        gender_unknown_value=_parse_choice(
            raw.get("gender_unknown_value", ""), {"", "U"}, f"{where}.gender_unknown_value"
        ),
        gender_missing_rate=_check_probability(
            _require(raw, "gender_missing_rate", where), f"{where}.gender_missing_rate"
        ),
    )


def _parse_pipeline_cases(raw: dict[str, Any]) -> PipelineCases:
    m = _require(raw, "migration", "pipeline_cases")
    where = "pipeline_cases.migration"
    for key in ("dob_format", "ssn_format"):
        if m.get(key) != "next":
            raise ConfigError(f"{where}.{key} must be 'next' (rotate to the next format)")
    default_ssn = str(_require(m, "default_ssn", where))
    if not (default_ssn.isdigit() and len(default_ssn) == 9):
        raise ConfigError(f"{where}.default_ssn must be nine digits")
    migration = Migration(
        name_case=_parse_choice(_require(m, "name_case", where), NAME_CASES, where),
        first_name_max_length=_check_positive_int(
            _require(m, "first_name_max_length", where), f"{where}.first_name_max_length"
        ),
        last_name_max_length=_check_positive_int(
            _require(m, "last_name_max_length", where), f"{where}.last_name_max_length"
        ),
        default_ssn=default_ssn,
        default_gender=_parse_choice(
            _require(m, "default_gender", where), {"U", "M", "F", "X"}, where
        ),
    )
    return PipelineCases(
        migration=migration,
        source_id_change_share=_check_probability(
            _require(raw, "source_id_change_share", "pipeline_cases"),
            "pipeline_cases.source_id_change_share",
        ),
        column_shift_batch_share=_check_probability(
            _require(raw, "column_shift_batch_share", "pipeline_cases"),
            "pipeline_cases.column_shift_batch_share",
        ),
    )


def _parse_edge_case(raw: dict[str, Any]) -> EdgeCase:
    code = _require(raw, "code", "edge_cases entry")
    where = f"edge case {code}"
    case = EdgeCase(
        code=code,
        category=_require(raw, "category", where),
        description=_require(raw, "description", where),
        generation=_require(raw, "generation", where),
        expected_outcome=_require(raw, "expected_outcome", where),
        unit=_require(raw, "unit", where),
        rate=_check_probability(_require(raw, "rate", where), f"{where}.rate"),
    )
    if case.category not in EDGE_CASE_CATEGORIES:
        raise ConfigError(f"{where}: unknown category {case.category!r}")
    if case.unit not in EDGE_CASE_UNITS:
        raise ConfigError(f"{where}: unknown unit {case.unit!r}")
    return case


def load_matching_config(path: Path = paths.MATCHING_CONFIG) -> MatchingConfig:
    """Load and validate the matching configuration."""
    raw = _read_yaml(path)

    profiles: dict[str, Profile] = {}
    for name, spec in _require(raw, "profiles", "matching config").items():
        where = f"profiles.{name}"
        tier_4 = _require(spec, "tier_4_matches", where)
        if not isinstance(tier_4, bool):
            raise ConfigError(f"{where}.tier_4_matches must be true or false")
        profile = Profile(
            name=name,
            strong_name_threshold=_check_probability(
                _require(spec, "strong_name_threshold", where), f"{where}.strong_name_threshold"
            ),
            weak_name_threshold=_check_probability(
                _require(spec, "weak_name_threshold", where), f"{where}.weak_name_threshold"
            ),
            tier_4_matches=tier_4,
            use_case=str(spec.get("use_case", "")),
        )
        if profile.weak_name_threshold >= profile.strong_name_threshold:
            raise ConfigError(f"{where}: weak_name_threshold must be below strong_name_threshold")
        profiles[name] = profile

    default_profile = _require(raw, "default_profile", "matching config")
    if default_profile not in profiles:
        raise ConfigError(f"default_profile {default_profile!r} is not a defined profile")

    names = _require(raw, "names", "matching config")
    blockers = _require(raw, "blockers", "matching config")
    blocking = _require(raw, "blocking", "matching config")
    person_id_raw = _require(raw, "person_id", "matching config")

    keys = tuple(_require(blocking, "keys", "blocking"))
    unknown = sorted(set(keys) - BLOCKING_KEYS)
    if unknown:
        raise ConfigError(f"Unknown blocking keys {unknown}; expected from {sorted(BLOCKING_KEYS)}")
    if len(set(keys)) != len(keys):
        raise ConfigError("blocking.keys must not repeat")

    prefix = _require(person_id_raw, "prefix", "person_id")
    if not isinstance(prefix, str) or not prefix.isalpha():
        raise ConfigError("person_id.prefix must be alphabetic")

    return MatchingConfig(
        default_profile=default_profile,
        profiles=profiles,
        name_prefix_min_length=_check_positive_int(
            _require(names, "prefix_min_length", "names"), "names.prefix_min_length"
        ),
        generational_gap_years=_check_positive_int(
            _require(blockers, "generational_gap_years", "blockers"),
            "blockers.generational_gap_years",
        ),
        newborn_max_age_years=_check_positive_int(
            _require(blockers, "newborn_max_age_years", "blockers"),
            "blockers.newborn_max_age_years",
        ),
        max_block_size=_check_positive_int(
            _require(blocking, "max_block_size", "blocking"), "blocking.max_block_size"
        ),
        blocking_keys=keys,
        person_id=PersonIdFormat(
            prefix=prefix,
            digits=_check_positive_int(_require(person_id_raw, "digits", "person_id"), "digits"),
        ),
    )
