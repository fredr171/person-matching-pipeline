"""Pipeline code must never read ground truth (DESIGN.md section 3.1).

This is a static tripwire: it fails if any source file in a pipeline package
mentions truth at all (a path, ``truth_dir``, a column such as
``true_person_id``). Only the generator (which writes truth), ``evaluation/``
and ``tests/`` may. It cannot prove the absence of a cleverly hidden read, but
it catches every honest mistake, which is the realistic risk.
"""

from __future__ import annotations

import re

import pytest

from pipeline import paths

# Packages and the dbt project that must stay blind to ground truth.
TRUTH_BLIND = ["ingestion", "standardization", "matching_dbt", "matching_spark", "clustering"]
SOURCE_SUFFIXES = {".py", ".sql", ".yml", ".yaml", ".csv", ".md"}
FORBIDDEN = re.compile(r"truth|true_person_id", re.IGNORECASE)
IGNORED_DIRS = {"target", "logs", "dbt_packages", "__pycache__"}


def _source_files(package: str):
    root = paths.REPO_ROOT / package
    for path in root.rglob("*"):
        if path.suffix in SOURCE_SUFFIXES and not IGNORED_DIRS & set(path.parts):
            yield path


@pytest.mark.parametrize("package", TRUTH_BLIND)
def test_package_never_references_truth(package):
    offenders = []
    for path in _source_files(package):
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if FORBIDDEN.search(line):
                offenders.append(
                    f"{path.relative_to(paths.REPO_ROOT)}:{line_number}: {line.strip()}"
                )
    assert offenders == [], "pipeline code references ground truth:\n" + "\n".join(offenders)


def test_tripwire_detects_a_reference(tmp_path):
    """Guard against the check silently matching nothing."""
    sample = 'df = pd.read_parquet(paths.truth_dir(3) / "truth.parquet")'
    assert FORBIDDEN.search(sample)
