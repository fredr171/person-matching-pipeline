"""Ingestion: CDC events -> current state (DESIGN.md section 4)."""

from __future__ import annotations

import shutil
from collections import defaultdict
from datetime import datetime

import pyarrow.parquet as pq
import pytest
from generator_helpers import file_hashes

from ingestion.apply import RecordState, apply_events
from ingestion.run import (
    CURRENT_SCHEMA,
    REJECT_SCHEMA,
    STATE_SCHEMA,
    ingest_all,
    ingest_events,
    ingest_load,
    read_events,
    read_state,
)
from ingestion.validate import validate_event
from pipeline import paths

# --------------------------------------------------------------------------- #
# Hand-built events
# --------------------------------------------------------------------------- #

_counter = 0


def event(op="I", seq=1, record="R1", event_id=None, **values) -> dict:
    """A well-formed event; override any column with keyword arguments."""
    global _counter
    _counter += 1
    row = {
        "event_id": event_id or f"E{_counter:06d}",
        "source_system": "EHR_A",
        "source_record_id": record,
        "sequence_no": seq,
        "event_ts": datetime(2025, 1, 1),
        "op": op,
        "first_name": "Ana",
        "last_name": "Lopez",
        "dob": "1980-05-06",
        "ssn": "123-45-6780",
        "gender": "F",
    }
    if op == "D":
        row.update(dict.fromkeys(("first_name", "last_name", "dob", "ssn", "gender")))
    row.update(values)
    return row


def run(*loads: list[dict]) -> tuple[dict, list]:
    """Ingest several loads of events into a fresh state; return state and all rejects."""
    state: dict = {}
    rejects = []
    for load_id, events in enumerate(loads):
        load_rejects, _ = ingest_events(state, events, load_id)
        rejects.extend(load_rejects)
    return state, rejects


def only(state) -> RecordState:
    assert len(state) == 1
    return next(iter(state.values()))


# --------------------------------------------------------------------------- #
# Step 1: validation
# --------------------------------------------------------------------------- #


def test_well_formed_events_pass():
    assert validate_event(event("I")) is None
    assert validate_event(event("U")) is None
    assert validate_event(event("D")) is None


@pytest.mark.parametrize("column", ["event_id", "source_system", "source_record_id", "sequence_no"])
@pytest.mark.parametrize("missing", [None, "", "  "])
def test_missing_key_is_rejected(column, missing):
    if column == "sequence_no" and missing is not None:
        pytest.skip("sequence_no is an integer column; only None applies")
    broken = event()
    broken[column] = missing
    rejection = validate_event(broken)
    assert rejection is not None and rejection.reason == "REJECT_MISSING_KEY"


@pytest.mark.parametrize("op", ["X", "", None, "i", "UPDATE"])
def test_bad_op_is_rejected(op):
    rejection = validate_event(event(op=op))
    assert rejection is not None and rejection.reason == "REJECT_BAD_OP"


def test_column_shift_is_rejected():
    """The generator's COLUMN_SHIFT: every value moved one column to the right."""
    shifted = event(
        first_name="", last_name="Ana", dob="Lopez", ssn="1980-05-06", gender="123-45-6780"
    )
    rejection = validate_event(shifted)
    assert rejection is not None and rejection.reason == "REJECT_COLUMN_SHIFT"
    assert "dob contains letters" in rejection.detail


@pytest.mark.parametrize(
    "dob",
    ["", "1900-01-01", "06/07/85", "1985", "19850607", "13/45/1980"],
)
def test_bad_dob_values_are_not_column_shifts(dob):
    """Value problems belong to standardization, not to ingestion."""
    assert validate_event(event(dob=dob)) is None


def test_dob_with_letters_alone_is_not_enough_for_a_shift():
    assert validate_event(event(dob="Jan 5 1980")) is None


@pytest.mark.parametrize("name", ["JosÃ©", "MÃ¼ller", "SÃ¸ren", "JoseÌ\u0081"])
def test_mojibake_is_rejected(name):
    rejection = validate_event(event(first_name=name))
    assert rejection is not None and rejection.reason == "REJECT_ENCODING"


@pytest.mark.parametrize("name", ["José", "MÜLLER", "Søren", "Østergaard", "José", "O'Brien"])
def test_legitimate_unicode_names_pass(name):
    assert validate_event(event(last_name=name)) is None


def test_control_characters_are_rejected():
    rejection = validate_event(event(last_name="Lo\u0000pez"))
    assert rejection is not None and rejection.reason == "REJECT_ENCODING"


def test_deletes_are_not_checked_for_values():
    assert validate_event(event("D", dob="Lopez", first_name="")) is None


# --------------------------------------------------------------------------- #
# Steps 2-4: deduplicate, order, apply
# --------------------------------------------------------------------------- #


def test_insert_creates_a_live_record():
    state, _ = run([event("I", first_name="Ana")])
    record = only(state)
    assert not record.is_deleted and record.record_life == 1 and record.first_name == "Ana"


def test_duplicate_delivery_in_one_load_is_applied_once():
    original = event("I")
    stats = apply_events({}, [original, dict(original)], 0)
    assert stats.duplicates_dropped == 1 and stats.applied["I"] == 1


def test_duplicate_delivery_in_a_later_load_is_a_no_op():
    original = event("U", seq=2, first_name="Anna")
    state, _ = run([event("I", seq=1), original], [dict(original)])
    assert only(state).first_name == "Anna" and only(state).last_changed_load == 0


def test_events_in_one_load_are_ordered_by_sequence_not_arrival():
    newest = event("U", seq=3, first_name="Third")
    state, _ = run([newest, event("U", seq=2, first_name="Second"), event("I", seq=1)])
    assert only(state).first_name == "Third" and only(state).sequence_no == 3


def test_out_of_order_event_arriving_a_load_late_is_ignored():
    state, _ = run(
        [event("I", seq=1), event("U", seq=3, first_name="New")],
        [event("U", seq=2, first_name="Old")],
    )
    assert only(state).first_name == "New"


def test_late_update_after_delete_is_ignored():
    state, _ = run([event("I", seq=1), event("D", seq=3)], [event("U", seq=2, first_name="Late")])
    assert only(state).is_deleted


def test_event_ts_is_informational_only():
    early_ts = event("U", seq=3, first_name="Wins", event_ts=datetime(2020, 1, 1))
    late_ts = event("U", seq=2, first_name="Loses", event_ts=datetime(2030, 1, 1))
    state, _ = run([event("I", seq=1), late_ts, early_ts])
    assert only(state).first_name == "Wins"


def test_insert_after_delete_starts_a_new_record_life():
    """SOURCE_ID_REUSE: the same key now holds a different record."""
    state, _ = run(
        [event("I", seq=1, first_name="Old")],
        [event("D", seq=2)],
        [event("I", seq=3, first_name="New")],
    )
    record = only(state)
    assert record.record_life == 2
    assert record.first_name == "New" and record.life_started_load == 2


def test_update_for_an_unseen_key_is_an_upsert():
    """E.g. the insert was rejected: the next full row image creates the record (D-027)."""
    shifted_insert = event("I", seq=1, first_name="", dob="Lopez", ssn="1980-05-06")
    state, rejects = run([shifted_insert], [event("U", seq=2, first_name="Ana")])
    assert [r["reason_code"] for r in rejects] == ["REJECT_COLUMN_SHIFT"]
    assert only(state).first_name == "Ana" and only(state).record_life == 1


def test_delete_of_an_unseen_key_leaves_a_tombstone():
    state, _ = run([event("D", seq=5)], [event("I", seq=4)])
    record = only(state)
    assert record.is_deleted and record.record_life == 0 and record.sequence_no == 5
    state2, _ = run([event("D", seq=5)], [event("I", seq=6)])
    assert not only(state2).is_deleted and only(state2).record_life == 1


def test_equal_sequence_numbers_keep_the_lowest_event_id():
    state, _ = run(
        [
            event("I", seq=1),
            event("U", seq=2, event_id="E-b", first_name="B"),
            event("U", seq=2, event_id="E-a", first_name="A"),
        ]
    )
    assert only(state).first_name == "A"


def test_rejected_events_do_not_change_state():
    state, rejects = run(
        [event("I", seq=1, first_name="Ana")], [event("U", seq=2, first_name="JosÃ©")]
    )
    assert only(state).first_name == "Ana" and len(rejects) == 1


def test_delete_clears_values():
    state, _ = run([event("I", seq=1), event("D", seq=2)])
    assert only(state).first_name is None and only(state).ssn is None


def test_reapplying_a_load_is_a_no_op():
    load = [event("I", seq=1), event("U", seq=2, first_name="X"), event("D", seq=1, record="R2")]
    state, _ = run(load)
    before = dict(state)
    stats = apply_events(state, [dict(e) for e in load], 1)
    assert state == before
    assert stats.stale_ignored == len(load) and sum(stats.applied.values()) == 0


# --------------------------------------------------------------------------- #
# Generated data: files, idempotency, rejects against ground truth
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="module")
def ingested_boosted(boosted_run, tmp_path_factory):
    """A private copy of the boosted run, ingested once."""
    data_dir = tmp_path_factory.mktemp("ingest") / "data"
    shutil.copytree(boosted_run / "loads", data_dir / "loads")
    metrics = ingest_all(data_dir)
    return data_dir, metrics


def _ingested_hashes(data_dir):
    return file_hashes(data_dir / "ingested")


def test_outputs_exist_with_expected_schemas(ingested_boosted):
    data_dir, metrics = ingested_boosted
    assert len(metrics) == len(list((data_dir / "loads").glob("load_*")))
    out = paths.ingested_dir(len(metrics) - 1, data_dir)
    assert pq.read_schema(out / "state.parquet").equals(STATE_SCHEMA)
    assert pq.read_schema(out / "current_records.parquet").equals(CURRENT_SCHEMA)
    assert pq.read_schema(out / "rejects.parquet").equals(REJECT_SCHEMA)


def test_current_records_are_the_live_part_of_state(ingested_boosted):
    data_dir, metrics = ingested_boosted
    for m in metrics:
        out = paths.ingested_dir(m.load_id, data_dir)
        current = pq.read_table(out / "current_records.parquet").to_pylist()
        keys = [(r["source_system"], r["source_record_id"]) for r in current]
        assert len(keys) == len(set(keys)) == m.live_records


def test_ingesting_everything_twice_gives_identical_files(ingested_boosted):
    data_dir, _ = ingested_boosted
    first = _ingested_hashes(data_dir)
    ingest_all(data_dir)
    assert _ingested_hashes(data_dir) == first


def test_reingesting_one_load_from_saved_state_is_identical(ingested_boosted):
    data_dir, metrics = ingested_boosted
    middle = len(metrics) // 2
    before = _ingested_hashes(data_dir)
    ingest_load(middle, data_dir)
    assert _ingested_hashes(data_dir) == before


def test_replaying_any_earlier_load_does_not_change_final_state(ingested_boosted):
    data_dir, metrics = ingested_boosted
    final = read_state(len(metrics) - 1, data_dir)
    for load_id in range(len(metrics)):
        state = dict(final)
        ingest_events(state, read_events(load_id, data_dir), len(metrics))
        assert state.keys() == final.keys()
        assert all(
            state[k].sequence_no == final[k].sequence_no
            and state[k].first_name == final[k].first_name
            for k in final
        )


def test_incremental_ingestion_equals_one_batch_of_all_events(ingested_boosted):
    """Load-by-load application gives the same result as applying everything at once."""
    data_dir, metrics = ingested_boosted
    incremental = read_state(len(metrics) - 1, data_dir)
    everything = [e for load_id in range(len(metrics)) for e in read_events(load_id, data_dir)]
    batch: dict = {}
    ingest_events(batch, everything, 0)

    def core(state):
        return {
            k: (s.record_life, s.is_deleted, s.sequence_no, s.first_name, s.last_name, s.dob, s.ssn)
            for k, s in state.items()
        }

    assert core(batch) == core(incremental)


def _rejects(data_dir, metrics):
    rows = []
    for m in metrics:
        rows += pq.read_table(
            paths.ingested_dir(m.load_id, data_dir) / "rejects.parquet"
        ).to_pylist()
    return rows


def _tags_by_key(truth):
    tags = defaultdict(set)
    for rows in truth.values():
        for row in rows:
            tags[(row["source_system"], row["source_record_id"])].update(row["edge_case_tags"])
    return tags


@pytest.mark.parametrize(
    ("reason", "tag"),
    [("REJECT_COLUMN_SHIFT", "COLUMN_SHIFT"), ("REJECT_ENCODING", "ENCODING_CORRUPTION")],
)
def test_rejects_match_ground_truth_exactly(ingested_boosted, boosted_truth, reason, tag):
    """Precision: only corrupted records are rejected. Recall: every one of them is."""
    data_dir, metrics = ingested_boosted
    tags = _tags_by_key(boosted_truth)
    rejects = _rejects(data_dir, metrics)
    rejected_keys = {
        (r["source_system"], r["source_record_id"]) for r in rejects if r["reason_code"] == reason
    }
    tagged_keys = {k for k, t in tags.items() if tag in t}
    assert rejected_keys, f"no {reason} rejects in the boosted run"
    assert rejected_keys <= tagged_keys, "rejected a record that was not corrupted"
    assert rejected_keys == tagged_keys, (
        f"missed {len(tagged_keys - rejected_keys)} corrupted records"
    )


def test_no_unexpected_reject_reasons(ingested_boosted):
    data_dir, metrics = ingested_boosted
    reasons = {r["reason_code"] for r in _rejects(data_dir, metrics)}
    assert reasons <= {"REJECT_COLUMN_SHIFT", "REJECT_ENCODING"}


def test_late_and_out_of_order_records_end_with_their_newest_values(
    ingested_boosted, boosted_truth
):
    """For records hit by OUT_OF_ORDER, the state equals the highest-sequence valid event."""
    data_dir, metrics = ingested_boosted
    tags = _tags_by_key(boosted_truth)
    final = read_state(len(metrics) - 1, data_dir)
    newest: dict = {}
    for load_id in range(len(metrics)):
        for e in read_events(load_id, data_dir):
            key = (e["source_system"], e["source_record_id"])
            if (
                validate_event(e) is None
                and e["sequence_no"] > newest.get(key, {"sequence_no": 0})["sequence_no"]
            ):
                newest[key] = e
    checked = 0
    for key, t in tags.items():
        if "OUT_OF_ORDER" in t and key in final:
            expected = newest[key]
            assert final[key].sequence_no == expected["sequence_no"]
            assert final[key].is_deleted == (expected["op"] == "D")
            checked += 1
    assert checked > 0


def test_ingesting_without_earlier_state_fails_clearly(tmp_path, small_run):
    data_dir, _ = small_run
    target = tmp_path / "data"
    shutil.copytree(data_dir / "loads", target / "loads")
    with pytest.raises(FileNotFoundError, match="ingest earlier loads first"):
        ingest_load(3, target)
