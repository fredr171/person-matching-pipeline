"""Blocking keys and candidate pairs (DESIGN.md section 6, D-040)."""

from __future__ import annotations

from matching_helpers import record

from standardization.blocking import (
    BLOCKING_KEYS,
    SkippedBlock,
    candidate_pairs,
    key_values,
    last_name_key,
    record_key,
)


def test_record_key_is_source_and_id():
    r = record(source_record_id="R1")
    assert record_key(r) == "EHR_A|R1"


def test_key_values_of_a_complete_record():
    r = record(last_name="O'Brien-Smith", first_name="Catherine")
    values = key_values(r)
    assert set(values) == set(BLOCKING_KEYS)
    assert values["ssn"] == "234567801"
    assert values["dob"] == "1980-05-06"
    assert values["ssn_sorted_digits_birth_year"] == "012345678|1980"
    assert values["ssn_first5_birth_year"] == "23456|1980"
    assert values["last_phonetic_first_initial_birth_year"].endswith("|C|1980")


def test_missing_fields_drop_the_keys_that_need_them():
    assert set(key_values(record(ssn=None))) == {"dob", "last_phonetic_first_initial_birth_year"}
    assert set(key_values(record(dob=None))) == {"ssn"}
    assert "last_phonetic_first_initial_birth_year" not in key_values(record(first_name=None))


def test_last_name_key_ignores_spaces_hyphens_and_apostrophes():
    assert last_name_key("O'BRIEN-SMITH") == last_name_key("OBRIEN SMITH") == "OBRIENSMITH"


def test_transposed_ssn_still_shares_the_sorted_digits_key():
    a, b = record(ssn="234-56-7801"), record(ssn="234-56-7810", dob="1980-07-06")
    shared = {k for k in BLOCKING_KEYS if key_values(a).get(k) == key_values(b).get(k)}
    assert shared == {
        "ssn_sorted_digits_birth_year",
        "ssn_first5_birth_year",
        "last_phonetic_first_initial_birth_year",
    }


def test_pairs_are_deduplicated_ordered_and_list_every_key():
    a, b = record(source_record_id="B"), record(source_record_id="A")
    candidates = candidate_pairs([a, b], max_block_size=10)
    assert candidates.pairs == {("EHR_A|A", "EHR_A|B"): tuple(sorted(BLOCKING_KEYS))}
    assert candidates.skipped_blocks == ()


def test_records_sharing_nothing_are_not_paired():
    a = record(source_record_id="A")
    b = record(source_record_id="B", ssn="345-67-8912", dob="1991-02-03", last_name="Okafor")
    assert candidate_pairs([a, b], max_block_size=10).pairs == {}


def test_oversized_blocks_are_skipped_and_reported():
    """Four records share an SSN but differ elsewhere; a cap of 3 skips the SSN block."""
    records = [
        record(source_record_id=f"R{i}", dob=f"19{60 + i}-0{i + 1}-1{i}", last_name=name)
        for i, name in enumerate(["Miller", "Okafor", "Novak", "Tanaka"])
    ]
    candidates = candidate_pairs(records, max_block_size=3)
    assert SkippedBlock("ssn", "234567801", 4) in candidates.skipped_blocks
    assert all("ssn" not in keys for keys in candidates.pairs.values())


def test_only_configured_keys_are_used():
    a, b = record(source_record_id="A"), record(source_record_id="B")
    candidates = candidate_pairs([a, b], max_block_size=10, keys=("dob",))
    assert candidates.pairs == {("EHR_A|A", "EHR_A|B"): ("dob",)}
