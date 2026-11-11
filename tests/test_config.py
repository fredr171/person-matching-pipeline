"""Configuration files load, validate, and agree with DESIGN.md."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

from pipeline import config, paths


def _design_section(design_doc: str, start_heading: str, end_heading: str) -> str:
    start = design_doc.index(start_heading)
    return design_doc[start : design_doc.index(end_heading, start)]


# --------------------------------------------------------------------------- #
# Generator config
# --------------------------------------------------------------------------- #


def test_edge_case_codes_match_design_catalog(generator_config, design_doc):
    """The config catalog and DESIGN.md section 3.7 list exactly the same codes (D-016)."""
    catalog = _design_section(design_doc, "### 3.7 Edge case catalog", "## 4.")
    design_codes = set(re.findall(r"^\| ([A-Z][A-Z0-9_]+) \|", catalog, flags=re.MULTILINE))
    config_codes = {case.code for case in generator_config.edge_cases}

    assert len(design_codes) > 50, "parsing DESIGN.md found suspiciously few codes"
    assert config_codes - design_codes == set(), "codes in config but not in DESIGN.md"
    assert design_codes - config_codes == set(), "codes in DESIGN.md but not in config"


def test_presets_match_design(generator_config):
    """Section 3.4 preset sizes."""
    small, medium, large = (generator_config.preset(n) for n in ("small", "medium", "large"))
    assert small.initial_records == 1_000
    assert medium.initial_records == 100_000
    assert (medium.refresh_records_min, medium.refresh_records_max) == (2_000, 5_000)
    assert large.initial_records == 5_000_000
    assert generator_config.default_preset == "medium"


def test_refresh_mix_matches_design(generator_config):
    mix = generator_config.refresh_mix
    assert (mix.insert, mix.update, mix.delete) == (0.70, 0.25, 0.05)


def test_records_per_person_mean_is_about_1_4(generator_config):
    """Section 3.5: average about 1.4 records per person."""
    assert generator_config.records_per_person.expected_mean() == pytest.approx(1.4, abs=0.05)


def test_one_source_writes_family_name_first(generator_config):
    """NAME_FAMILY_FIRST needs exactly one systematic source."""
    assert sum(s.family_name_first for s in generator_config.source_systems) == 1


def test_unknown_preset_is_rejected(generator_config):
    with pytest.raises(config.ConfigError, match="Unknown preset"):
        generator_config.preset("huge")


# --------------------------------------------------------------------------- #
# Matching config
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("name", "strong", "weak", "tier_4"),
    [
        ("conservative", 0.95, 0.88, False),
        ("balanced", 0.92, 0.85, False),
        ("aggressive", 0.88, 0.82, True),
    ],
)
def test_profiles_match_design(matching_config, name, strong, weak, tier_4):
    """Section 12 profile table."""
    profile = matching_config.profile(name)
    assert profile.strong_name_threshold == strong
    assert profile.weak_name_threshold == weak
    assert profile.tier_4_matches is tier_4


def test_matching_defaults_match_design(matching_config):
    assert matching_config.default_profile == "conservative"
    assert matching_config.generational_gap_years == 12
    assert matching_config.name_prefix_min_length == 6
    assert set(matching_config.blocking_keys) == config.BLOCKING_KEYS


def test_person_id_format(matching_config):
    assert matching_config.person_id.format(123) == "P000000123"
    with pytest.raises(ValueError):
        matching_config.person_id.format(0)


# --------------------------------------------------------------------------- #
# Validation rejects bad values
# --------------------------------------------------------------------------- #


def _write_modified(tmp_path: Path, source: Path, mutate) -> Path:
    data = yaml.safe_load(source.read_text(encoding="utf-8"))
    mutate(data)
    target = tmp_path / source.name
    target.write_text(yaml.safe_dump(data), encoding="utf-8")
    return target


def test_weak_threshold_above_strong_is_rejected(tmp_path):
    def mutate(data):
        data["profiles"]["balanced"]["weak_name_threshold"] = 0.99

    path = _write_modified(tmp_path, paths.MATCHING_CONFIG, mutate)
    with pytest.raises(config.ConfigError, match="weak_name_threshold must be below"):
        config.load_matching_config(path)


def test_unknown_blocking_key_is_rejected(tmp_path):
    def mutate(data):
        data["blocking"]["keys"].append("zip_code")

    path = _write_modified(tmp_path, paths.MATCHING_CONFIG, mutate)
    with pytest.raises(config.ConfigError, match="Unknown blocking keys"):
        config.load_matching_config(path)


def test_refresh_mix_not_summing_to_one_is_rejected(tmp_path):
    def mutate(data):
        data["refresh_mix"]["delete"] = 0.5

    path = _write_modified(tmp_path, paths.GENERATOR_CONFIG, mutate)
    with pytest.raises(config.ConfigError, match="refresh_mix must sum to 1"):
        config.load_generator_config(path)


def test_rate_outside_unit_interval_is_rejected(tmp_path):
    def mutate(data):
        data["edge_cases"][0]["rate"] = 1.5

    path = _write_modified(tmp_path, paths.GENERATOR_CONFIG, mutate)
    with pytest.raises(config.ConfigError, match="between 0 and 1"):
        config.load_generator_config(path)


def test_duplicate_edge_case_code_is_rejected(tmp_path):
    def mutate(data):
        data["edge_cases"].append(dict(data["edge_cases"][0]))

    path = _write_modified(tmp_path, paths.GENERATOR_CONFIG, mutate)
    with pytest.raises(config.ConfigError, match="Duplicate edge case codes"):
        config.load_generator_config(path)


def test_unknown_edge_case_unit_is_rejected(tmp_path):
    def mutate(data):
        data["edge_cases"][0]["unit"] = "household"

    path = _write_modified(tmp_path, paths.GENERATOR_CONFIG, mutate)
    with pytest.raises(config.ConfigError, match="unknown unit"):
        config.load_generator_config(path)
