"""Shared pytest fixtures."""

from __future__ import annotations

import shutil

import pytest
from generator_helpers import boosted, read_loads

from generator.generate import generate
from ingestion.run import ingest_all
from pipeline import config, paths, reference


@pytest.fixture(scope="session")
def generator_config() -> config.GeneratorConfig:
    return config.load_generator_config()


@pytest.fixture(scope="session")
def matching_config() -> config.MatchingConfig:
    return config.load_matching_config()


@pytest.fixture(scope="session")
def reference_data() -> reference.ReferenceData:
    return reference.load_reference_data()


@pytest.fixture(scope="session")
def design_doc() -> str:
    return (paths.REPO_ROOT / "docs" / "DESIGN.md").read_text(encoding="utf-8")


# --------------------------------------------------------------------------- #
# Fixtures: one normal and one boosted small run per test session
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="session")
def small_run(tmp_path_factory, generator_config):
    data_dir = tmp_path_factory.mktemp("small")
    summary = generate(generator_config, "small", data_dir=data_dir)
    return data_dir, summary


@pytest.fixture(scope="session")
def boosted_run(tmp_path_factory, generator_config):
    data_dir = tmp_path_factory.mktemp("boosted")
    generate(boosted(generator_config), "small", data_dir=data_dir)
    return data_dir


@pytest.fixture(scope="session")
def boosted_events(boosted_run):
    return read_loads(boosted_run, "loads", "events.parquet")


@pytest.fixture(scope="session")
def boosted_truth(boosted_run):
    return read_loads(boosted_run, "truth", "truth.parquet")


@pytest.fixture(scope="session")
def ingested_boosted(boosted_run, tmp_path_factory):
    """A private copy of the boosted run, ingested once."""
    data_dir = tmp_path_factory.mktemp("ingest") / "data"
    shutil.copytree(boosted_run / "loads", data_dir / "loads")
    metrics = ingest_all(data_dir)
    return data_dir, metrics
