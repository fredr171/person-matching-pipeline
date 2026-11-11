"""Metric definitions against ground truth (DESIGN.md section 14, D-043).

Everything here works on :class:`EvalRecord` rows: one per live record, with
the person ID the pipeline assigned and the person the record really belongs
to. No pairs are enumerated: pair counts follow from group sizes, because a
group of ``n`` records holds ``n * (n - 1) / 2`` pairs.

Terms used throughout:

* *predicted partners* of a record: the other records with its person ID.
* *true partners*: the other live records of its true person.
* *correct partners*: records that are both.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from fractions import Fraction

CORRECT = "CORRECT"
MISSED_MATCH = "MISSED_MATCH"
FALSE_MERGE = "FALSE_MERGE"

CORRECTIVE = "CORRECTIVE"
DEGRADING = "DEGRADING"
NEUTRAL = "NEUTRAL"


@dataclass(frozen=True)
class EvalRecord:
    """A live record with its assigned and its true person."""

    record_uid: str
    record_key: str
    person_id: str
    true_person_id: str
    edge_case_tags: tuple[str, ...] = ()


def pairs_in(size: int) -> int:
    """Number of unordered pairs in a group of ``size`` records."""
    return size * (size - 1) // 2


def _ratio(numerator: int, denominator: int) -> float | None:
    return numerator / denominator if denominator else None


@dataclass(frozen=True)
class PairCounts:
    """Pairwise counts: correct pairs, predicted pairs and true pairs."""

    true_positive: int
    predicted: int
    actual: int

    @property
    def precision(self) -> float | None:
        """Share of predicted same-person pairs that are really the same person."""
        return _ratio(self.true_positive, self.predicted)

    @property
    def recall(self) -> float | None:
        """Share of true same-person pairs the pipeline put together."""
        return _ratio(self.true_positive, self.actual)

    @property
    def f1(self) -> float | None:
        return _ratio(2 * self.true_positive, self.predicted + self.actual)

    @property
    def false_positive(self) -> int:
        return self.predicted - self.true_positive

    @property
    def false_negative(self) -> int:
        return self.actual - self.true_positive

    def as_dict(self) -> dict:
        return {
            "true_positive": self.true_positive,
            "predicted": self.predicted,
            "actual": self.actual,
            "precision": self.precision,
            "recall": self.recall,
            "f1": self.f1,
        }


@dataclass(frozen=True)
class _Groups:
    """Group sizes: by assigned person, by true person, and by both."""

    predicted: Counter
    actual: Counter
    cells: Counter

    @classmethod
    def of(cls, records: Iterable[EvalRecord]) -> _Groups:
        predicted, actual, cells = Counter(), Counter(), Counter()
        for r in records:
            predicted[r.person_id] += 1
            actual[r.true_person_id] += 1
            cells[(r.person_id, r.true_person_id)] += 1
        return cls(predicted, actual, cells)


def pairwise(records: Iterable[EvalRecord]) -> PairCounts:
    """Pairwise precision and recall over all pairs of live records."""
    groups = _Groups.of(records)
    return PairCounts(
        true_positive=sum(pairs_in(n) for n in groups.cells.values()),
        predicted=sum(pairs_in(n) for n in groups.predicted.values()),
        actual=sum(pairs_in(n) for n in groups.actual.values()),
    )


# --------------------------------------------------------------------------- #
# Per record
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class RecordOutcome:
    """How one record ended up, counted in partners."""

    record: EvalRecord
    correct_partners: int
    predicted_partners: int
    true_partners: int

    @property
    def outcome(self) -> str:
        """FALSE_MERGE if any predicted partner is wrong; else MISSED_MATCH if a
        true partner is missing; else CORRECT."""
        if self.correct_partners < self.predicted_partners:
            return FALSE_MERGE
        if self.correct_partners < self.true_partners:
            return MISSED_MATCH
        return CORRECT

    @property
    def has_true_partner(self) -> bool:
        return self.true_partners > 0

    @property
    def f1(self) -> Fraction:
        """The record's pairwise F1 over its own pairs; 1 for a correct singleton."""
        total = self.predicted_partners + self.true_partners
        return Fraction(2 * self.correct_partners, total) if total else Fraction(1)


def record_outcomes(records: Iterable[EvalRecord]) -> dict[str, RecordOutcome]:
    """Outcome of every record, keyed by record uid."""
    records = list(records)
    groups = _Groups.of(records)
    return {
        r.record_uid: RecordOutcome(
            record=r,
            correct_partners=groups.cells[(r.person_id, r.true_person_id)] - 1,
            predicted_partners=groups.predicted[r.person_id] - 1,
            true_partners=groups.actual[r.true_person_id] - 1,
        )
        for r in records
    }


# --------------------------------------------------------------------------- #
# Per edge case
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class EdgeCaseOutcome:
    """Outcomes of the records tagged with one edge case code (D-043).

    Records with a true partner can be correct, a missed match or a false
    merge. Records without one (the only live record of their person) can
    only be correct or a false merge. ``pairs`` counts every pair involving at
    least one tagged record, each pair once.
    """

    code: str
    records: int
    with_partner: int
    correct: int
    missed_match: int
    false_merge: int
    without_partner: int
    without_partner_false_merge: int
    pairs: PairCounts

    def as_dict(self) -> dict:
        row = {k: v for k, v in self.__dict__.items() if k != "pairs"}
        pairs = self.pairs.as_dict()
        return {**row, **{f"pair_{k}": v for k, v in pairs.items()}}


def _pairs_touching(sizes: Mapping, tagged: Mapping) -> int:
    """Pairs with at least one tagged member, given each group's size and tagged count."""
    return sum(pairs_in(sizes[g]) - pairs_in(sizes[g] - k) for g, k in tagged.items())


def edge_case_outcomes(outcomes: Mapping[str, RecordOutcome]) -> dict[str, EdgeCaseOutcome]:
    """One :class:`EdgeCaseOutcome` per code that appears on any live record."""
    groups = _Groups.of(o.record for o in outcomes.values())
    by_code: dict[str, list[RecordOutcome]] = defaultdict(list)
    for o in outcomes.values():
        for code in o.record.edge_case_tags:
            by_code[code].append(o)

    result = {}
    for code, tagged in sorted(by_code.items()):
        partnered = [o for o in tagged if o.has_true_partner]
        alone = [o for o in tagged if not o.has_true_partner]
        kinds = Counter(o.outcome for o in partnered)
        tagged_in = (
            Counter(o.record.person_id for o in tagged),
            Counter(o.record.true_person_id for o in tagged),
            Counter((o.record.person_id, o.record.true_person_id) for o in tagged),
        )
        result[code] = EdgeCaseOutcome(
            code=code,
            records=len(tagged),
            with_partner=len(partnered),
            correct=kinds[CORRECT],
            missed_match=kinds[MISSED_MATCH],
            false_merge=kinds[FALSE_MERGE],
            without_partner=len(alone),
            without_partner_false_merge=sum(o.outcome == FALSE_MERGE for o in alone),
            pairs=PairCounts(
                true_positive=_pairs_touching(groups.cells, tagged_in[2]),
                predicted=_pairs_touching(groups.predicted, tagged_in[0]),
                actual=_pairs_touching(groups.actual, tagged_in[1]),
            ),
        )
    return result


# --------------------------------------------------------------------------- #
# Entity instability between consecutive loads
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class IdChange:
    """A record whose person ID changed from the previous load."""

    record_uid: str
    previous_person_id: str
    person_id: str
    previous_f1: Fraction
    f1: Fraction

    @property
    def kind(self) -> str:
        """CORRECTIVE if the record's F1 improved, DEGRADING if it fell, else NEUTRAL."""
        if self.f1 > self.previous_f1:
            return CORRECTIVE
        if self.f1 < self.previous_f1:
            return DEGRADING
        return NEUTRAL


@dataclass(frozen=True)
class Instability:
    """ID churn between two loads, over records live in both."""

    records_in_both: int
    changes: tuple[IdChange, ...]

    @property
    def changed(self) -> int:
        return len(self.changes)

    def count(self, kind: str) -> int:
        return sum(c.kind == kind for c in self.changes)

    @property
    def churn_rate(self) -> float | None:
        return _ratio(self.changed, self.records_in_both)

    def as_dict(self) -> dict:
        return {
            "records_in_both": self.records_in_both,
            "changed": self.changed,
            "corrective": self.count(CORRECTIVE),
            "degrading": self.count(DEGRADING),
            "neutral": self.count(NEUTRAL),
            "churn_rate": self.churn_rate,
        }


def instability(
    previous: Mapping[str, RecordOutcome], current: Mapping[str, RecordOutcome]
) -> Instability:
    """Compare person IDs of records (by record uid) live in both loads.

    Each load's F1 is measured against that load's truth.
    """
    shared = sorted(previous.keys() & current.keys())
    changes = tuple(
        IdChange(
            record_uid=uid,
            previous_person_id=previous[uid].record.person_id,
            person_id=current[uid].record.person_id,
            previous_f1=previous[uid].f1,
            f1=current[uid].f1,
        )
        for uid in shared
        if previous[uid].record.person_id != current[uid].record.person_id
    )
    return Instability(records_in_both=len(shared), changes=changes)
