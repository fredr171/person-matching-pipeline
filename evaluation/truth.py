"""Ground truth as of a load (DESIGN.md section 3.3, D-025).

Truth rows are versioned: a row is written when a record life starts or its
tags change, and the truth for a record at load N is its latest row with
``load_id <= N``. Tags accumulate over a record's life, so a tag means "this
case affected the record at some point", not "the case is visible right now".
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq

from evaluation.metrics import EvalRecord
from pipeline import paths


@dataclass(frozen=True)
class Truth:
    true_person_id: str
    edge_case_tags: tuple[str, ...]


def truth_at(load_id: int, data_dir: Path = paths.DEFAULT_DATA_DIR) -> dict[tuple[str, str], Truth]:
    """Truth effective at ``load_id``, keyed by (source_system, source_record_id)."""
    truth: dict[tuple[str, str], Truth] = {}
    for n in range(load_id + 1):
        path = paths.truth_dir(n, data_dir) / "truth.parquet"
        if not path.is_file():
            raise FileNotFoundError(f"Missing ground truth for load {n}: {path}")
        table = pq.read_table(
            path, columns=["source_system", "source_record_id", "true_person_id", "edge_case_tags"]
        ).to_pydict()
        for source, record_id, person, tags in zip(*table.values(), strict=True):
            truth[(source, record_id)] = Truth(person, tuple(tags))
    return truth


def evaluation_records(
    assignment_path: Path, truth: dict[tuple[str, str], Truth]
) -> list[EvalRecord]:
    """One :class:`EvalRecord` per row of a ``person_assignment`` output."""
    columns = ["source_system", "source_record_id", "record_key", "record_uid", "person_id"]
    table = pq.read_table(assignment_path, columns=columns).to_pydict()
    records = []
    for source, record_id, key, uid, person_id in zip(*table.values(), strict=True):
        found = truth.get((source, record_id))
        if found is None:
            raise ValueError(f"Live record {key} has no ground truth; was the data regenerated?")
        records.append(EvalRecord(uid, key, person_id, found.true_person_id, found.edge_case_tags))
    return records
