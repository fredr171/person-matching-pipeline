"""Where true pairs are lost or false pairs made: blocking, pair decisions, clustering.

The final pairwise metrics (``metrics.pairwise``) say how good the clusters are.
These say which stage is responsible:

* *blocking recall*: share of true same-person pairs that were compared at all.
* *decision precision / recall*: the ``is_match`` pair decisions, before clustering.
* *reason codes*: for each reason code, how many pairs were really the same
  person. ``SKIPPED_TRANSITIVITY`` joins refused by clustering are included.

Pairs are joined to truth in DuckDB, because ``match_pairs`` is much larger than
the record table.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
import pyarrow as pa

from evaluation.metrics import EvalRecord, PairCounts


@dataclass(frozen=True)
class ReasonCodeTruth:
    reason_code: str
    pairs: int
    same_person: int
    different_person: int

    def as_dict(self) -> dict:
        return dict(self.__dict__)


@dataclass(frozen=True)
class StageMetrics:
    compared_pairs: int
    true_pairs_compared: int
    true_pairs: int
    decisions: PairCounts
    reason_codes: tuple[ReasonCodeTruth, ...]

    @property
    def blocking_recall(self) -> float | None:
        return self.true_pairs_compared / self.true_pairs if self.true_pairs else None

    def as_dict(self) -> dict:
        return {
            "compared_pairs": self.compared_pairs,
            "true_pairs_compared": self.true_pairs_compared,
            "blocking_recall": self.blocking_recall,
            "decisions": self.decisions.as_dict(),
        }


def stage_metrics(records: list[EvalRecord], true_pairs: int, matched_dir: Path) -> StageMetrics:
    """Join one load's ``match_pairs`` and ``skipped_joins`` to the records' truth.

    ``true_pairs`` is the number of true same-person pairs among ``records``.
    """
    people = pa.table(
        {
            "record_key": [r.record_key for r in records],
            "true_person_id": [r.true_person_id for r in records],
        }
    )
    with duckdb.connect() as con:
        con.register("people", people)
        pairs_path = (matched_dir / "match_pairs.parquet").as_posix()
        skipped_path = (matched_dir / "skipped_joins.parquet").as_posix()
        con.execute(
            f"""
            create table judged as
            select p.reason_code, p.is_match, a.true_person_id = b.true_person_id as same_person
            from read_parquet('{pairs_path}') p
            join people a on a.record_key = p.record_key_a
            join people b on b.record_key = p.record_key_b
            union all
            select 'SKIPPED_TRANSITIVITY', false, a.true_person_id = b.true_person_id
            from read_parquet('{skipped_path}') s
            join people a on a.record_key = s.record_key_a
            join people b on b.record_key = s.record_key_b
            """
        )
        compared, true_compared, matched, true_matched = con.execute(
            """
            select
                count(*) filter (where reason_code <> 'SKIPPED_TRANSITIVITY'),
                count(*) filter (where reason_code <> 'SKIPPED_TRANSITIVITY' and same_person),
                count(*) filter (where is_match),
                count(*) filter (where is_match and same_person)
            from judged
            """
        ).fetchall()[0]
        reasons = con.execute(
            """
            select reason_code, count(*), count(*) filter (where same_person),
                   count(*) filter (where not same_person)
            from judged group by reason_code order by reason_code
            """
        ).fetchall()
    return StageMetrics(
        compared_pairs=compared,
        true_pairs_compared=true_compared,
        true_pairs=true_pairs,
        decisions=PairCounts(true_matched, matched, true_pairs),
        reason_codes=tuple(ReasonCodeTruth(*row) for row in reasons),
    )
