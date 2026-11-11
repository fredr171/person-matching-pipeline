"""Canonical filesystem locations.

Every stage resolves paths through this module so the directory layout in
DESIGN.md section 2 is defined in exactly one place.

Ground truth lives in its own tree (``data/truth/``) so that pipeline code
never has a reason to look there. Only the generator (which writes it),
``evaluation/`` and the tests may call :func:`truth_dir`. A test enforces this
(``tests/test_truth_isolation.py``).
"""

from pathlib import Path

REPO_ROOT: Path = Path(__file__).resolve().parents[1]

CONFIG_DIR: Path = REPO_ROOT / "config"
REFERENCE_DIR: Path = CONFIG_DIR / "reference"
GENERATOR_CONFIG: Path = CONFIG_DIR / "generator.yaml"
MATCHING_CONFIG: Path = CONFIG_DIR / "matching.yaml"

DBT_PROJECT_DIR: Path = REPO_ROOT / "matching_dbt"

DEFAULT_DATA_DIR: Path = REPO_ROOT / "data"


def load_dir(load_id: int, data_dir: Path = DEFAULT_DATA_DIR) -> Path:
    """Directory holding one load's CDC events: ``data/loads/load_NNNN/``.

    Load 0 is the initial load; loads 1..N are monthly refreshes.
    """
    if load_id < 0:
        raise ValueError(f"load_id must be non-negative, got {load_id}")
    return data_dir / "loads" / f"load_{load_id:04d}"


def ingested_dir(load_id: int, data_dir: Path = DEFAULT_DATA_DIR) -> Path:
    """Directory holding ingestion output after one load: ``data/ingested/load_NNNN/``."""
    if load_id < 0:
        raise ValueError(f"load_id must be non-negative, got {load_id}")
    return data_dir / "ingested" / f"load_{load_id:04d}"


def truth_dir(load_id: int, data_dir: Path = DEFAULT_DATA_DIR) -> Path:
    """Directory holding one load's ground truth: ``data/truth/load_NNNN/``.

    Restricted: only the generator, ``evaluation/`` and tests may use this.
    """
    if load_id < 0:
        raise ValueError(f"load_id must be non-negative, got {load_id}")
    return data_dir / "truth" / f"load_{load_id:04d}"
