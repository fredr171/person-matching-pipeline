"""Evaluate matched loads against ground truth (DESIGN.md section 14).

Reads only matching outputs (``data/matched/<engine>/<profile>/load_NNNN/``)
and truth, so it scores dbt and PySpark the same way. For each load it writes
to ``data/evaluation/<engine>/<profile>/load_NNNN/``:

* ``summary.json``: pairwise metrics, record outcomes, stage metrics, ID churn.
* ``edge_cases.parquet``: outcomes per edge case code.
* ``reason_codes.parquet``: pairs per reason code that were really the same person.
* ``record_outcomes.parquet``: every record's outcome, for drilling into errors.
* ``id_changes.parquet``: every person ID change since the previous load, classified.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from evaluation.metrics import (
    EdgeCaseOutcome,
    Instability,
    PairCounts,
    RecordOutcome,
    edge_case_outcomes,
    instability,
    pairwise,
    record_outcomes,
)
from evaluation.stages import StageMetrics, stage_metrics
from evaluation.truth import evaluation_records, truth_at
from pipeline import paths


@dataclass(frozen=True)
class LoadEvaluation:
    load_id: int
    engine: str
    profile: str
    pairwise: PairCounts
    outcomes: dict[str, RecordOutcome]
    edge_cases: dict[str, EdgeCaseOutcome]
    stages: StageMetrics
    instability: Instability | None

    @property
    def outcome_counts(self) -> dict[str, int]:
        return dict(sorted(Counter(o.outcome for o in self.outcomes.values()).items()))

    def summary(self) -> dict:
        return {
            "load_id": self.load_id,
            "engine": self.engine,
            "profile": self.profile,
            "records": len(self.outcomes),
            "pairwise": self.pairwise.as_dict(),
            "record_outcomes": self.outcome_counts,
            "record_outcomes_by_partner": {
                side: dict(
                    sorted(
                        Counter(
                            o.outcome
                            for o in self.outcomes.values()
                            if o.has_true_partner == partnered
                        ).items()
                    )
                )
                for side, partnered in (("with_partner", True), ("without_partner", False))
            },
            "stages": self.stages.as_dict(),
            "instability": self.instability.as_dict() if self.instability else None,
        }


def matched_loads(engine: str, profile: str, data_dir: Path) -> list[int]:
    root = data_dir / "matched" / engine / profile
    return sorted(
        int(p.name.split("_")[1])
        for p in root.glob("load_*")
        if (p / "person_assignment.parquet").is_file()
    )


def _outcomes_for(engine: str, profile: str, load_id: int, data_dir: Path):
    matched = paths.matched_dir(engine, profile, load_id, data_dir)
    records = evaluation_records(matched / "person_assignment.parquet", truth_at(load_id, data_dir))
    return records, record_outcomes(records)


def evaluate_load(
    load_id: int,
    engine: str = "dbt",
    profile: str = "conservative",
    data_dir: Path = paths.DEFAULT_DATA_DIR,
    previous: dict[str, RecordOutcome] | None = None,
) -> LoadEvaluation:
    """Evaluate one matched load. ``previous`` (the prior load's outcomes) is
    recomputed when not given."""
    records, outcomes = _outcomes_for(engine, profile, load_id, data_dir)
    if previous is None and load_id > 0:
        previous = _outcomes_for(engine, profile, load_id - 1, data_dir)[1]
    counts = pairwise(records)
    matched = paths.matched_dir(engine, profile, load_id, data_dir)
    return LoadEvaluation(
        load_id=load_id,
        engine=engine,
        profile=profile,
        pairwise=counts,
        outcomes=outcomes,
        edge_cases=edge_case_outcomes(outcomes),
        stages=stage_metrics(records, counts.actual, matched),
        instability=instability(previous, outcomes) if previous is not None else None,
    )


# --------------------------------------------------------------------------- #
# Writing
# --------------------------------------------------------------------------- #

EDGE_CASE_SCHEMA = pa.schema(
    [
        (name, pa.string() if name == "code" else pa.int64())
        for name in (
            "code",
            "records",
            "with_partner",
            "correct",
            "missed_match",
            "false_merge",
            "without_partner",
            "without_partner_false_merge",
            "pair_true_positive",
            "pair_predicted",
            "pair_actual",
        )
    ]
    + [("pair_precision", pa.float64()), ("pair_recall", pa.float64()), ("pair_f1", pa.float64())]
)
REASON_CODE_SCHEMA = pa.schema(
    [
        ("reason_code", pa.string()),
        ("pairs", pa.int64()),
        ("same_person", pa.int64()),
        ("different_person", pa.int64()),
    ]
)
RECORD_OUTCOME_SCHEMA = pa.schema(
    [
        ("record_uid", pa.string()),
        ("record_key", pa.string()),
        ("person_id", pa.string()),
        ("true_person_id", pa.string()),
        ("edge_case_tags", pa.list_(pa.string())),
        ("correct_partners", pa.int64()),
        ("predicted_partners", pa.int64()),
        ("true_partners", pa.int64()),
        ("outcome", pa.string()),
    ]
)
ID_CHANGE_SCHEMA = pa.schema(
    [
        ("record_uid", pa.string()),
        ("previous_person_id", pa.string()),
        ("person_id", pa.string()),
        ("previous_f1", pa.float64()),
        ("f1", pa.float64()),
        ("kind", pa.string()),
    ]
)


def _write(rows: list[dict], schema: pa.Schema, path: Path) -> None:
    pq.write_table(pa.Table.from_pylist(rows, schema), path)


def write_evaluation(evaluation: LoadEvaluation, data_dir: Path) -> Path:
    out = paths.evaluation_dir(evaluation.engine, evaluation.profile, evaluation.load_id, data_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "summary.json").write_text(
        json.dumps(evaluation.summary(), indent=2) + "\n", encoding="utf-8", newline="\n"
    )
    _write(
        [e.as_dict() for e in evaluation.edge_cases.values()],
        EDGE_CASE_SCHEMA,
        out / "edge_cases.parquet",
    )
    _write(
        [r.as_dict() for r in evaluation.stages.reason_codes],
        REASON_CODE_SCHEMA,
        out / "reason_codes.parquet",
    )
    _write(
        [
            {
                "record_uid": uid,
                "record_key": o.record.record_key,
                "person_id": o.record.person_id,
                "true_person_id": o.record.true_person_id,
                "edge_case_tags": list(o.record.edge_case_tags),
                "correct_partners": o.correct_partners,
                "predicted_partners": o.predicted_partners,
                "true_partners": o.true_partners,
                "outcome": o.outcome,
            }
            for uid, o in sorted(evaluation.outcomes.items())
        ],
        RECORD_OUTCOME_SCHEMA,
        out / "record_outcomes.parquet",
    )
    changes = evaluation.instability.changes if evaluation.instability else ()
    _write(
        [
            {
                "record_uid": c.record_uid,
                "previous_person_id": c.previous_person_id,
                "person_id": c.person_id,
                "previous_f1": float(c.previous_f1),
                "f1": float(c.f1),
                "kind": c.kind,
            }
            for c in changes
        ],
        ID_CHANGE_SCHEMA,
        out / "id_changes.parquet",
    )
    return out


def evaluate_all(
    engine: str = "dbt",
    profile: str = "conservative",
    data_dir: Path = paths.DEFAULT_DATA_DIR,
    through_load: int | None = None,
) -> Iterator[LoadEvaluation]:
    """Evaluate and write every matched load in order; yields each evaluation."""
    loads = matched_loads(engine, profile, data_dir)
    if not loads:
        raise FileNotFoundError(
            f"No matched loads for {engine}/{profile} under {data_dir / 'matched'}; "
            f"run `match-{engine}` first"
        )
    previous: LoadEvaluation | None = None
    for load_id in loads:
        if through_load is not None and load_id > through_load:
            break
        # Reuse the previous load's outcomes for ID churn when it was just evaluated.
        reuse = previous.outcomes if previous and previous.load_id == load_id - 1 else None
        evaluation = evaluate_load(load_id, engine, profile, data_dir, reuse)
        write_evaluation(evaluation, data_dir)
        previous = evaluation
        yield evaluation


def read_summary(engine: str, profile: str, load_id: int, data_dir: Path) -> dict:
    path = paths.evaluation_dir(engine, profile, load_id, data_dir) / "summary.json"
    return json.loads(path.read_text(encoding="utf-8"))
