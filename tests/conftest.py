"""Shared pytest fixtures."""

from __future__ import annotations

import pytest

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
