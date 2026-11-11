"""The dbt implementation reproduces the Python reference exactly (build phase 5).

dbt runs on (a) the first loads of the boosted generated run and (b) a
hand-made load of tricky raw values. Every layer is compared with the Python
reference: standardized records, candidate pairs, every field level and
decision, clusters and skipped joins, and person ID reconciliation. Then
re-runs check idempotency and ID stability.

These start dbt, so they carry the ``toolchain`` marker.
"""

from __future__ import annotations

import json
import shutil
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from clustering.constrained import CannotLink, MatchEdge, cluster_records
from clustering.ids import reconcile
from pipeline import config, paths, reference
from pipeline.match_dbt import OUTPUT_TABLES, load_date, match_load
from standardization.blocking import candidate_pairs, record_key
from standardization.rules import PairRules, decide
from standardization.standardize import StandardizationTables, standardize_record

pytestmark = pytest.mark.toolchain

LOADS = 3  # loads of the boosted run matched with dbt


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


def _read(output_dir: Path, table: str) -> list[dict]:
    return pq.read_table(output_dir / f"{table}.parquet").to_pylist()


def _by_uid(output_dir: Path) -> dict[str, str]:
    """record uid -> person id from one load's person_assignment."""
    return {a["record_uid"]: a["person_id"] for a in _read(output_dir, "person_assignment")}


@pytest.fixture(scope="module")
def dbt_boosted(ingested_boosted, tmp_path_factory):
    """The first LOADS loads of the boosted run, ingested and matched with dbt."""
    source, _ = ingested_boosted
    data_dir = tmp_path_factory.mktemp("dbt") / "data"
    for load_id in range(LOADS):
        for getter in (paths.load_dir, paths.ingested_dir):
            shutil.copytree(getter(load_id, source), getter(load_id, data_dir))
    runs = [match_load(load_id, data_dir) for load_id in range(LOADS)]
    return data_dir, runs


TRICKY_FIRST = [
    "José",
    "José",
    "SØREN",
    "Mary-Ann",
    "MaryAnn",
    "Mary Ann",
    "D.",
    "David",
    "Dave",
    "Chris",
    "Christina",
    "Baby Boy",
    "FNU",
    "Zoë Ἀλέξανδρος",
    "'Neil",
    "Anne--Marie",
    "Catherine",
    "Katherine",
    "Catherime",
    "MAXIMI",
    "Maximilian",
    "John III",
    "",
    None,
]
TRICKY_LAST = [
    "Müller",
    "Strauß",
    "O’Brien",
    "OBrien",
    "De la Cruz",
    "Delacruz",
    "Cruz",
    "García López",
    "Lopez",
    "Smith, Jr.",
    "Smith Sr",
    "SMITH JUNIOR",
    "Smyth",
    "-Smith-",
    "LNU",
    "Li (Lee)",
    "Van der Berg",
    "Al Hassan",
    None,
]
TRICKY_SSN = [
    "234-56-7801",
    "234 56 7801",
    "234567801",
    "234-56-7811",
    "234-56-7810",
    "12345678",
    "12345678.0",
    "1.23456780E+07",
    "2.345678E+08",
    "2.345678015E+08",
    "N/A",
    "234-56-780",
    "000-00-0000",
    "123-45-6789",
    "912-70-1234",
    "666-12-3456",
    "234-00-5678",
    "234-56-0000",
    " 234-56-7801\t",
    "",
    None,
]
TRICKY_DOB = [
    "1980-05-06",
    "1980-5-6",
    "1980-05-06 00:00:00",
    "05/06/1980",
    "19800506",
    "06/05/1980",
    "1980-05-08",
    "1981-05-06",
    "1984-02-29",
    "1984-02-28",
    "1984-03-01",
    "1900-01-01",
    "1980-01-01",
    "05/06/80",
    "05/06/15",
    "1980",
    "1980-02-30",
    "0000-01-01",
    "2025-06-02",
    "May 6 1980",
    "2025-03-01",
    "1990-03-01",
    "",
    None,
]
TRICKY_GENDER = ["F", "M", "f", " Female ", "X", "U", "?", "Q", "", None]
TRICKY_TODAY = date(2025, 6, 1)


def _tricky_rows() -> list[dict]:
    """Deterministic mix of tricky values, dense enough that many pairs share keys."""
    columns = [TRICKY_FIRST, TRICKY_LAST, TRICKY_SSN, TRICKY_DOB, TRICKY_GENDER]
    rows = []
    for i in range(600):
        first, last, ssn, dob, gender = (
            col[(i * step) % len(col)] for col, step in zip(columns, (1, 3, 7, 5, 2), strict=True)
        )
        rows.append(
            {
                "source_system": ["EHR_A", "CLAIMS_B", "LAB_C"][i % 3],
                "source_record_id": f"T{i:05d}",
                "record_life": 1 + i % 2,
                "first_name": first,
                "last_name": last,
                "dob": dob,
                "ssn": ssn,
                "gender": gender,
            }
        )
    # Hand-built variants of one person so every tier and blocker occurs.
    base = {
        "first_name": "Catherine",
        "last_name": "Miller",
        "dob": "1980-05-06",
        "ssn": "345-67-8901",
        "gender": "F",
    }
    variants = [
        {},
        {"ssn": "345-67-8911"},
        {"ssn": "345-67-8910"},
        {"dob": "1980-05-08"},
        {"dob": "1980-06-05"},
        {"first_name": "Katherine", "ssn": "345-67-8911"},
        {"first_name": "Miller", "last_name": "Catherine"},
        {"last_name": "Okafor"},
        {"first_name": "Emma", "ssn": "456-78-9012"},
        {"first_name": "Derek", "last_name": "Novak", "dob": "1972-09-14"},
        {"last_name": "Miller Jr"},
        {"last_name": "Miller Sr"},
        {"first_name": "Baby Girl", "dob": "2025-03-01"},
        {"ssn": ""},
        {"dob": "1980-01-01"},
    ]
    for i, change in enumerate(variants):
        rows.append(
            {
                "source_system": "EHR_A",
                "source_record_id": f"V{i:05d}",
                "record_life": 1,
                **base,
                **change,
            }
        )
    return rows


@pytest.fixture(scope="module")
def dbt_tricky(tmp_path_factory):
    """One load of tricky values, matched with dbt."""
    data_dir = tmp_path_factory.mktemp("tricky") / "data"
    rows = _tricky_rows()
    schema = pa.schema(
        [
            ("source_system", pa.string()),
            ("source_record_id", pa.string()),
            ("record_life", pa.int64()),
            ("first_name", pa.string()),
            ("last_name", pa.string()),
            ("dob", pa.string()),
            ("ssn", pa.string()),
            ("gender", pa.string()),
        ]
    )
    ingested = paths.ingested_dir(0, data_dir)
    ingested.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(rows, schema), ingested / "current_records.parquet")
    manifest = paths.load_dir(0, data_dir)
    manifest.mkdir(parents=True)
    (manifest / "manifest.json").write_text(json.dumps({"load_date": TRICKY_TODAY.isoformat()}))
    return data_dir, rows, match_load(0, data_dir)


# --------------------------------------------------------------------------- #
# Comparison helpers
# --------------------------------------------------------------------------- #

STANDARD_FIELDS = (
    "first_name",
    "last_name",
    "first_name_forms",
    "last_name_forms",
    "first_name_is_placeholder",
    "last_name_is_placeholder",
    "suffix",
    "ssn",
    "ssn_null_reason",
    "dob",
    "dob_null_reason",
    "dob_is_jan1",
    "gender",
)
PAIR_FIELDS = (
    "ssn_level",
    "dob_level",
    "dob_gap_years",
    "first_level",
    "first_method",
    "last_level",
    "last_method",
    "names_swapped",
)


def _python_records(rows, today):
    tables = StandardizationTables.from_reference(reference.load_reference_data())
    return {record_key(r): r for r in (standardize_record(row, tables, today) for row in rows)}


def assert_standardization_matches(dbt_rows, python_records):
    assert len(dbt_rows) == len(python_records)
    for row in dbt_rows:
        expected = python_records[row["record_key"]]
        for field in STANDARD_FIELDS:
            actual = row[field]
            if field.endswith("_forms"):
                actual = tuple(actual)
            assert actual == getattr(expected, field), (row["record_key"], field, row)


def assert_pairs_match(output_dir, python_records, profile="conservative"):
    matching = config.load_matching_config()
    rules = PairRules.build(reference.load_reference_data(), matching, profile)
    today = date.fromisoformat(load_date(_load_of(output_dir), output_dir.parents[3]))
    dbt_pairs = _read(output_dir, "match_pairs")

    expected = candidate_pairs(python_records.values(), matching.max_block_size)
    assert {
        (p["record_key_a"], p["record_key_b"]): tuple(p["blocking_keys"]) for p in dbt_pairs
    } == expected.pairs
    assert {
        (b["key_name"], b["key_value"], b["block_size"])
        for b in _read(output_dir, "skipped_blocks")
    } == {(b.key_name, b.key_value, b.block_size) for b in expected.skipped_blocks}

    for pair in dbt_pairs:
        a, b = python_records[pair["record_key_a"]], python_records[pair["record_key_b"]]
        decision = decide(a, b, rules, today)
        for field in PAIR_FIELDS:
            assert pair[field] == getattr(decision.comparison, field), (field, pair)
        assert (
            pair["tier"],
            tuple(pair["blockers"]),
            pair["reason_code"],
            pair["is_match"],
            pair["cannot_link"],
        ) == (
            decision.tier,
            decision.blockers,
            decision.reason_code,
            decision.is_match,
            decision.cannot_link,
        ), pair
    return dbt_pairs


def _load_of(output_dir: Path) -> int:
    return int(output_dir.name.split("_")[1])


# --------------------------------------------------------------------------- #
# Tricky values
# --------------------------------------------------------------------------- #


def test_tricky_values_standardize_like_the_reference(dbt_tricky):
    data_dir, rows, run = dbt_tricky
    dbt_rows = _read(run.output_dir, "records_standardized")
    assert_standardization_matches(dbt_rows, _python_records(rows, TRICKY_TODAY))


def test_tricky_values_compare_like_the_reference(dbt_tricky):
    data_dir, rows, run = dbt_tricky
    pairs = assert_pairs_match(run.output_dir, _python_records(rows, TRICKY_TODAY))
    reasons = {p["reason_code"] for p in pairs}
    assert len(pairs) > 1000
    assert {
        "TIER_1_EXACT_IDS",
        "TIER_2_SSN_NEAR",
        "TIER_3_DOB_NEAR",
        "TIER_4_NO_SSN",
        "BLOCKED_SUFFIX",
        "BLOCKED_GENERATIONAL",
        "BLOCKED_TWINS",
        "BLOCKED_SSN_CONFLICT",
        "NO_MATCH_INSUFFICIENT",
    } <= reasons
    assert any(p["names_swapped"] for p in pairs)


# --------------------------------------------------------------------------- #
# Generated data, load by load
# --------------------------------------------------------------------------- #


def _current_rows(data_dir, load_id):
    return pq.read_table(
        paths.ingested_dir(load_id, data_dir) / "current_records.parquet"
    ).to_pylist()


@pytest.mark.parametrize("load_id", range(LOADS))
def test_generated_load_matches_the_reference(dbt_boosted, load_id):
    data_dir, runs = dbt_boosted
    output_dir = runs[load_id].output_dir
    today = date.fromisoformat(load_date(load_id, data_dir))
    python_records = _python_records(_current_rows(data_dir, load_id), today)
    assert_standardization_matches(_read(output_dir, "records_standardized"), python_records)
    pairs = assert_pairs_match(output_dir, python_records)

    # Clustering: the shared function on the same decisions gives the same clusters.
    clustering = cluster_records(
        python_records,
        [
            MatchEdge(p["record_key_a"], p["record_key_b"], p["tier"])
            for p in pairs
            if p["is_match"]
        ],
        [
            CannotLink(p["record_key_a"], p["record_key_b"], p["reason_code"])
            for p in pairs
            if p["cannot_link"]
        ],
    )
    clusters = {c["record_key"]: c["cluster_id"] for c in _read(output_dir, "clusters")}
    assert clusters == clustering.cluster_of
    skipped = _read(output_dir, "skipped_joins")
    assert sorted((s["record_key_a"], s["record_key_b"]) for s in skipped) == sorted(
        (s.record_key_a, s.record_key_b) for s in clustering.skipped
    )

    # Reconciliation: same ids as the shared function given the previous load.
    assignment = _read(output_dir, "person_assignment")
    uid_of = {a["record_key"]: a["record_uid"] for a in assignment}
    if load_id == 0:
        previous, next_sequence = {}, 1
    else:
        previous_dir = runs[load_id - 1].output_dir
        previous = {
            a["record_uid"]: a["person_id"] for a in _read(previous_dir, "person_assignment")
        }
        next_sequence = _read(previous_dir, "id_state")[0]["next_sequence"]
    expected = reconcile(
        {uid_of[k]: c for k, c in clusters.items()},
        previous,
        next_sequence,
        config.load_matching_config().person_id,
    )
    assert {a["record_uid"]: a["person_id"] for a in assignment} == expected.person_of
    crosswalk = {
        (c["retired_id"], c["successor_id"], c["reason"])
        for c in _read(output_dir, "person_id_crosswalk")
    }
    assert crosswalk == {(r.retired_id, r.successor_id, r.reason) for r in expected.retirements}
    assert _read(output_dir, "id_state")[0]["next_sequence"] == expected.next_sequence


def test_every_output_table_is_written(dbt_boosted):
    _, runs = dbt_boosted
    for run in runs:
        for table in OUTPUT_TABLES:
            assert (run.output_dir / f"{table}.parquet").is_file(), table


def test_no_cluster_contains_a_blocked_pair(dbt_boosted):
    _, runs = dbt_boosted
    for run in runs:
        clusters = {c["record_key"]: c["cluster_id"] for c in _read(run.output_dir, "clusters")}
        for pair in _read(run.output_dir, "match_pairs"):
            if pair["cannot_link"]:
                assert clusters[pair["record_key_a"]] != clusters[pair["record_key_b"]]


def test_run_metrics_agree_with_the_outputs(dbt_boosted):
    _, runs = dbt_boosted
    for run in runs:
        pairs = _read(run.output_dir, "match_pairs")
        m = run.metrics
        assert m[("pairs", "compared")] == len(pairs)
        assert m[("pairs", "matched")] == sum(p["is_match"] for p in pairs)
        assert m[("records", "live_records")] == len(_read(run.output_dir, "person_assignment"))
        for code in {p["reason_code"] for p in pairs}:
            assert m[("reason_code", code)] == sum(p["reason_code"] == code for p in pairs)


# --------------------------------------------------------------------------- #
# Idempotency and stability (DESIGN.md section 15)
# --------------------------------------------------------------------------- #


def _as_set(output_dir, table):
    return {json.dumps(row, sort_keys=True, default=str) for row in _read(output_dir, table)}


def test_rerunning_a_load_reproduces_its_outputs(dbt_boosted):
    data_dir, runs = dbt_boosted
    last = runs[-1].output_dir
    before = {table: _as_set(last, table) for table in OUTPUT_TABLES}
    match_load(len(runs) - 1, data_dir)
    assert {table: _as_set(last, table) for table in OUTPUT_TABLES} == before


def test_matching_the_same_records_again_changes_no_person_id(dbt_boosted):
    """A load identical to the previous one: zero id changes, new ids or retirements."""
    data_dir, runs = dbt_boosted
    repeat = len(runs)
    shutil.copytree(paths.ingested_dir(repeat - 1, data_dir), paths.ingested_dir(repeat, data_dir))
    shutil.copytree(paths.load_dir(repeat - 1, data_dir), paths.load_dir(repeat, data_dir))
    run = match_load(repeat, data_dir)
    assert run.metrics[("person_ids", "records_with_changed_id")] == 0
    assert run.metrics[("person_ids", "new")] == 0
    assert _read(run.output_dir, "person_id_crosswalk") == []
    previous = {
        a["record_uid"]: a["person_id"] for a in _read(runs[-1].output_dir, "person_assignment")
    }
    assert {
        a["record_uid"]: a["person_id"] for a in _read(run.output_dir, "person_assignment")
    } == previous


def test_aggressive_profile_matches_the_reference(dbt_boosted, tmp_path):
    """Profile thresholds and Tier 4 reach dbt through vars from config/matching.yaml."""
    data_dir, _ = dbt_boosted
    run = match_load(0, data_dir, "aggressive")
    assert run.output_dir.parent.name == "aggressive"
    today = date.fromisoformat(load_date(0, data_dir))
    pairs = assert_pairs_match(
        run.output_dir, _python_records(_current_rows(data_dir, 0), today), "aggressive"
    )
    assert any(p["reason_code"] == "TIER_4_NO_SSN" and p["is_match"] for p in pairs)


def test_matching_a_load_before_its_predecessor_fails_clearly(tmp_path, dbt_boosted):
    data_dir, _ = dbt_boosted
    target = tmp_path / "data"
    for getter in (paths.load_dir, paths.ingested_dir):
        shutil.copytree(getter(1, data_dir), getter(1, target))
    with pytest.raises(FileNotFoundError, match="match earlier loads first"):
        match_load(1, target)
