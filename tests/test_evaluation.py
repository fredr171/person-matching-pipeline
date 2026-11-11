"""Evaluation against ground truth (DESIGN.md section 14, D-043).

Hand-built cases pin each definition; brute-force pair enumeration checks the
counting shortcuts; an end-to-end run matches two small loads with every
profile, evaluates them and renders the results page.
"""

from __future__ import annotations

import itertools
import json
import random
import shutil
import xml.etree.ElementTree as ET
from fractions import Fraction

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from evaluation.chart import Point, precision_recall_svg
from evaluation.metrics import (
    CORRECT,
    CORRECTIVE,
    DEGRADING,
    FALSE_MERGE,
    MISSED_MATCH,
    NEUTRAL,
    EvalRecord,
    edge_case_outcomes,
    instability,
    pairwise,
    record_outcomes,
)
from evaluation.truth import evaluation_records, truth_at
from pipeline import config, paths


def rec(uid, person_id, true_person_id, *tags):
    return EvalRecord(uid, uid, person_id, true_person_id, tuple(tags))


# True people: A = r1, r2, r3; B = r4; C = r5, r6. Predicted: P1 = r1, r2, r4; P2 = r3; P3 = r5, r6.
EXAMPLE = [
    rec("r1", "P1", "A", "TWINS"),
    rec("r2", "P1", "A"),
    rec("r3", "P2", "A", "TWINS"),
    rec("r4", "P1", "B", "TWINS"),
    rec("r5", "P3", "C"),
    rec("r6", "P3", "C"),
]


# --------------------------------------------------------------------------- #
# Pairwise and per record
# --------------------------------------------------------------------------- #


def test_pairwise_counts_on_a_hand_built_example():
    counts = pairwise(EXAMPLE)
    # Correct: r1-r2, r5-r6. Predicted: r1-r2, r1-r4, r2-r4, r5-r6.
    # True: r1-r2, r1-r3, r2-r3, r5-r6.
    assert (counts.true_positive, counts.predicted, counts.actual) == (2, 4, 4)
    assert counts.precision == counts.recall == counts.f1 == 0.5


def test_empty_counts_have_no_ratios():
    counts = pairwise([rec("r1", "P1", "A")])
    assert counts.precision is None and counts.recall is None and counts.f1 is None


def _brute_force(records):
    tp = predicted = actual = 0
    for a, b in itertools.combinations(records, 2):
        same_predicted, same_true = a.person_id == b.person_id, a.true_person_id == b.true_person_id
        predicted += same_predicted
        actual += same_true
        tp += same_predicted and same_true
    return tp, predicted, actual


def _random_records(seed, n=120):
    rng = random.Random(seed)
    tags = ["TWINS", "MARRIAGE", "NAME_TYPO"]
    return [
        rec(
            f"r{i:03d}",
            f"P{rng.randint(1, 40)}",
            f"T{rng.randint(1, 45)}",
            *rng.sample(tags, rng.randint(0, 2)),
        )
        for i in range(n)
    ]


@pytest.mark.parametrize("seed", range(5))
def test_pairwise_equals_pair_enumeration(seed):
    records = _random_records(seed)
    counts = pairwise(records)
    assert (counts.true_positive, counts.predicted, counts.actual) == _brute_force(records)


def test_record_outcomes():
    outcomes = record_outcomes(EXAMPLE)
    assert {uid: o.outcome for uid, o in outcomes.items()} == {
        "r1": FALSE_MERGE,  # with r4, who is someone else
        "r2": FALSE_MERGE,
        "r3": MISSED_MATCH,  # alone, but r1 and r2 are the same person
        "r4": FALSE_MERGE,  # B has no other record, but is merged into A's
        "r5": CORRECT,
        "r6": CORRECT,
    }
    r1 = outcomes["r1"]
    assert (r1.correct_partners, r1.predicted_partners, r1.true_partners) == (1, 2, 2)
    assert r1.f1 == Fraction(1, 2)
    assert not outcomes["r4"].has_true_partner


def test_a_correct_singleton_has_f1_one():
    (outcome,) = record_outcomes([rec("r1", "P1", "A")]).values()
    assert outcome.outcome == CORRECT and outcome.f1 == 1


# --------------------------------------------------------------------------- #
# Per edge case
# --------------------------------------------------------------------------- #


def test_edge_case_outcomes_split_records_with_and_without_partner():
    twins = edge_case_outcomes(record_outcomes(EXAMPLE))["TWINS"]
    assert twins.records == 3
    assert (twins.with_partner, twins.correct, twins.missed_match, twins.false_merge) == (
        2,
        0,
        1,
        1,
    )
    assert (twins.without_partner, twins.without_partner_false_merge) == (1, 1)
    # Pairs touching r1, r3 or r4: predicted r1-r2, r1-r4, r2-r4; true r1-r2, r1-r3, r2-r3.
    assert (twins.pairs.true_positive, twins.pairs.predicted, twins.pairs.actual) == (1, 3, 3)


@pytest.mark.parametrize("seed", range(5))
def test_edge_case_pairs_count_each_touching_pair_once(seed):
    records = _random_records(seed)
    outcomes = edge_case_outcomes(record_outcomes(records))
    for code, outcome in outcomes.items():
        tp = predicted = actual = 0
        for a, b in itertools.combinations(records, 2):
            if code not in a.edge_case_tags and code not in b.edge_case_tags:
                continue
            same_predicted = a.person_id == b.person_id
            same_true = a.true_person_id == b.true_person_id
            predicted += same_predicted
            actual += same_true
            tp += same_predicted and same_true
        assert (outcome.pairs.true_positive, outcome.pairs.predicted, outcome.pairs.actual) == (
            tp,
            predicted,
            actual,
        ), code
        assert outcome.records == outcome.with_partner + outcome.without_partner
        assert outcome.with_partner == outcome.correct + outcome.missed_match + outcome.false_merge


# --------------------------------------------------------------------------- #
# Instability
# --------------------------------------------------------------------------- #


def test_id_changes_are_classified_by_the_records_own_f1():
    before = record_outcomes(
        [
            rec("a1", "P1", "A"),
            rec("a2", "P2", "A"),
            rec("b1", "P3", "B"),
            rec("c1", "P4", "C"),
            rec("d1", "P5", "D"),
            rec("gone", "P6", "E"),
        ]
    )
    after = record_outcomes(
        [
            rec("a1", "P1", "A"),
            rec("a2", "P1", "A"),  # joins its true partner: corrective
            rec("b1", "P4", "B"),  # merged into C's ID: degrading
            rec("c1", "P4", "C"),  # unchanged ID, but now falsely merged
            rec("d1", "P7", "D"),  # new ID, still correct alone: neutral
            rec("new", "P8", "F"),
        ]
    )
    result = instability(before, after)
    assert result.records_in_both == 5
    assert {c.record_uid: c.kind for c in result.changes} == {
        "a2": CORRECTIVE,
        "b1": DEGRADING,
        "d1": NEUTRAL,
    }
    assert result.as_dict()["churn_rate"] == 3 / 5


# --------------------------------------------------------------------------- #
# Truth
# --------------------------------------------------------------------------- #


def _write_truth(data_dir, load_id, rows):
    target = paths.truth_dir(load_id, data_dir)
    target.mkdir(parents=True)
    schema = pa.schema(
        [
            ("source_system", pa.string()),
            ("source_record_id", pa.string()),
            ("true_person_id", pa.string()),
            ("edge_case_tags", pa.list_(pa.string())),
            ("load_id", pa.int32()),
        ]
    )
    rows = [dict(zip(schema.names, (*row, load_id), strict=True)) for row in rows]
    pq.write_table(pa.Table.from_pylist(rows, schema), target / "truth.parquet")


def test_truth_is_the_latest_row_effective_at_the_load(tmp_path):
    _write_truth(tmp_path, 0, [("EHR_A", "1", "T1", []), ("EHR_A", "2", "T2", [])])
    _write_truth(tmp_path, 1, [("EHR_A", "1", "T1", ["MARRIAGE"])])
    _write_truth(tmp_path, 2, [("EHR_A", "1", "T9", ["SOURCE_ID_REUSE"])])
    assert truth_at(0, tmp_path)[("EHR_A", "1")].edge_case_tags == ()
    assert truth_at(1, tmp_path)[("EHR_A", "1")].edge_case_tags == ("MARRIAGE",)
    assert truth_at(2, tmp_path)[("EHR_A", "1")].true_person_id == "T9"
    assert truth_at(2, tmp_path)[("EHR_A", "2")].true_person_id == "T2"
    with pytest.raises(FileNotFoundError):
        truth_at(3, tmp_path)


def test_a_live_record_without_truth_is_an_error(tmp_path):
    _write_truth(tmp_path, 0, [("EHR_A", "1", "T1", [])])
    assignment = tmp_path / "person_assignment.parquet"
    pq.write_table(
        pa.table(
            {
                "source_system": ["EHR_A"],
                "source_record_id": ["2"],
                "record_key": ["EHR_A|2"],
                "record_uid": ["EHR_A|2|1"],
                "person_id": ["P000000001"],
            }
        ),
        assignment,
    )
    with pytest.raises(ValueError, match="no ground truth"):
        evaluation_records(assignment, truth_at(0, tmp_path))


# --------------------------------------------------------------------------- #
# Chart
# --------------------------------------------------------------------------- #

POINTS = [
    Point("conservative", 0.68, 0.992),
    Point("balanced", 0.70, 0.990),
    Point("aggressive", 0.78, 0.975),
]


def test_chart_is_valid_svg_with_every_point_labeled():
    svg = precision_recall_svg(POINTS, "Precision and recall")
    root = ET.fromstring(svg)
    assert root.tag.endswith("svg")
    circles = [c for c in root.iter() if c.tag.endswith("circle") and "ring" in c.get("class", "")]
    assert len(circles) == 3
    for point in POINTS:
        assert f">{point.label}</text>" in svg
    assert "prefers-color-scheme: dark" in svg
    assert svg == precision_recall_svg(POINTS, "Precision and recall")


def test_chart_axes_contain_every_point():
    svg = precision_recall_svg(POINTS, "t")
    root = ET.fromstring(svg)
    for c in root.iter():
        if c.tag.endswith("circle") and "ring" in c.get("class", ""):
            assert 72 <= float(c.get("cx")) <= 640 - 32
            assert 56 <= float(c.get("cy")) <= 420 - 64


def test_labels_of_nearly_equal_points_go_on_opposite_sides():
    svg = precision_recall_svg(
        [Point("conservative", 0.7102, 0.9916), Point("balanced", 0.7120, 0.9915)], "t"
    )
    anchors = [
        t.get("text-anchor")
        for t in ET.fromstring(svg).iter()
        if t.tag.endswith("text")
        and t.text in {"conservative", "balanced"}
        and t.get("class") == "ink"
    ]
    assert sorted(anchors) == ["end", "start"]


def test_chart_refuses_more_than_three_series():
    with pytest.raises(ValueError):
        precision_recall_svg([*POINTS, Point("fourth", 0.5, 0.5)], "t")


# --------------------------------------------------------------------------- #
# End to end: match, evaluate and report on two small loads
# --------------------------------------------------------------------------- #

LOADS = 2


@pytest.fixture(scope="module")
def evaluated(small_run, tmp_path_factory):
    """Two small loads ingested, matched with every profile and evaluated."""
    from evaluation.run import evaluate_all
    from ingestion.run import ingest_all
    from pipeline.match_dbt import match_all

    source, _ = small_run
    data_dir = tmp_path_factory.mktemp("evaluation") / "data"
    for load_id in range(LOADS):
        shutil.copytree(paths.load_dir(load_id, source), paths.load_dir(load_id, data_dir))
        shutil.copytree(paths.truth_dir(load_id, source), paths.truth_dir(load_id, data_dir))
    shutil.copy(source / "loads" / "generation.json", data_dir / "loads" / "generation.json")
    list(ingest_all(data_dir))
    evaluations = {}
    for profile in config.load_matching_config().profiles:
        list(match_all(data_dir, profile))
        evaluations[profile] = list(evaluate_all("dbt", profile, data_dir))
    return data_dir, evaluations


@pytest.mark.toolchain
def test_evaluation_files_are_written_and_consistent(evaluated):
    data_dir, evaluations = evaluated
    for profile, runs in evaluations.items():
        assert [e.load_id for e in runs] == list(range(LOADS))
        for e in runs:
            out = paths.evaluation_dir("dbt", profile, e.load_id, data_dir)
            summary = json.loads((out / "summary.json").read_text())
            outcomes = pq.read_table(out / "record_outcomes.parquet").to_pylist()
            assert summary["records"] == len(outcomes) == len(e.outcomes)
            # The pairwise numbers follow from the record outcomes.
            assert sum(o["correct_partners"] for o in outcomes) == 2 * e.pairwise.true_positive
            assert sum(o["predicted_partners"] for o in outcomes) == 2 * e.pairwise.predicted
            assert summary["stages"]["true_pairs_compared"] <= e.pairwise.actual
            reasons = pq.read_table(out / "reason_codes.parquet").to_pylist()
            compared = sum(
                r["pairs"] for r in reasons if r["reason_code"] != "SKIPPED_TRANSITIVITY"
            )
            assert compared == summary["stages"]["compared_pairs"]
            assert (summary["instability"] is None) == (e.load_id == 0)


@pytest.mark.toolchain
def test_pair_decision_metrics_agree_with_match_pairs(evaluated):
    data_dir, evaluations = evaluated
    e = evaluations["conservative"][-1]
    truth = {o.record.record_key: o.record.true_person_id for o in e.outcomes.values()}
    pairs = pq.read_table(
        paths.matched_dir("dbt", "conservative", e.load_id, data_dir) / "match_pairs.parquet"
    ).to_pylist()
    matched = [p for p in pairs if p["is_match"]]
    correct = sum(truth[p["record_key_a"]] == truth[p["record_key_b"]] for p in matched)
    assert (e.stages.decisions.true_positive, e.stages.decisions.predicted) == (
        correct,
        len(matched),
    )


@pytest.mark.toolchain
def test_aggressive_profile_finds_more_pairs(evaluated):
    _, evaluations = evaluated
    conservative, aggressive = evaluations["conservative"][-1], evaluations["aggressive"][-1]
    assert aggressive.pairwise.predicted >= conservative.pairwise.predicted
    assert aggressive.pairwise.recall >= conservative.pairwise.recall


@pytest.mark.toolchain
def test_results_page_covers_every_profile_and_edge_case(evaluated, tmp_path):
    from evaluation.report import CHART_NAME, write_results

    data_dir, _ = evaluated
    target = write_results(data_dir, "dbt", tmp_path)
    text = target.read_text(encoding="utf-8")
    for profile in config.load_matching_config().profiles:
        assert profile in text
    for case in config.load_generator_config().edge_cases:
        assert f"`{case.code}`" in text
    for heading in (
        "## Summary by profile",
        "## Where pairs are won and lost",
        "## Outcomes per edge case",
        "## Entity instability",
    ):
        assert heading in text
    ET.fromstring((tmp_path / CHART_NAME).read_text(encoding="utf-8"))
    assert write_results(data_dir, "dbt", tmp_path).read_text(encoding="utf-8") == text


@pytest.mark.toolchain
def test_evaluating_an_unmatched_profile_fails_clearly(tmp_path):
    from evaluation.run import evaluate_all

    with pytest.raises(FileNotFoundError, match="match-dbt"):
        list(evaluate_all("dbt", "conservative", tmp_path))
