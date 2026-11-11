"""The synthetic data generator (DESIGN.md section 3)."""

from __future__ import annotations

import csv
import os
import subprocess
import sys
from collections import defaultdict
from datetime import timedelta

import pyarrow.parquet as pq
import pytest
from generator_helpers import boosted, file_hashes, make_context, read_loads
from metaphone import doublemetaphone

from generator import catalog
from generator.docs import DOC_PATH, render_markdown
from generator.generate import generate
from generator.names import DATA_DIR, NamePools
from generator.population import build_population
from generator.record_cases import (
    FIRST_NAME_PLACEHOLDERS,
    FULL_NAME_PLACEHOLDERS,
    LAST_NAME_PLACEHOLDERS,
)
from generator.rng import stream
from generator.simulate import plan_refresh_ops
from generator.timeline import schedule_life_events
from generator.writer import EVENT_SCHEMA, TRUTH_SCHEMA
from pipeline import paths

# --------------------------------------------------------------------------- #
# Fixtures: one normal and one boosted small run per test session
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="session")
def small_run(tmp_path_factory, generator_config):
    data_dir = tmp_path_factory.mktemp("small")
    summary = generate(generator_config, "small", data_dir=data_dir)
    return data_dir, summary


@pytest.fixture(scope="session")
def boosted_run(tmp_path_factory, generator_config):
    data_dir = tmp_path_factory.mktemp("boosted")
    generate(boosted(generator_config), "small", data_dir=data_dir)
    return data_dir


@pytest.fixture(scope="session")
def boosted_events(boosted_run):
    return read_loads(boosted_run, "loads", "events.parquet")


@pytest.fixture(scope="session")
def boosted_truth(boosted_run):
    return read_loads(boosted_run, "truth", "truth.parquet")


def _latest_truth(truth: dict[int, list[dict]]) -> dict[tuple[str, str], dict]:
    latest = {}
    for load in sorted(truth):
        for row in truth[load]:
            latest[(row["source_system"], row["source_record_id"])] = row
    return latest


# --------------------------------------------------------------------------- #
# Catalog coverage
# --------------------------------------------------------------------------- #


def test_every_catalog_code_is_implemented_exactly_once(generator_config):
    implemented = catalog.implementations()
    assert set(implemented) == {c.code for c in generator_config.edge_cases}


def test_implementation_kind_matches_rate_unit(generator_config):
    allowed = {
        "record": {"record"},
        "person": {"population", "timeline"},
        "event": {"pipeline"},
        "load": {"pipeline"},
    }
    implemented = catalog.implementations()
    mismatched = [
        (c.code, c.unit, implemented[c.code])
        for c in generator_config.edge_cases
        if implemented[c.code] not in allowed[c.unit]
    ]
    assert mismatched == []


def test_every_edge_case_occurs_with_boosted_rates(boosted_truth, generator_config):
    seen = {tag for rows in boosted_truth.values() for row in rows for tag in row["edge_case_tags"]}
    missing = sorted({c.code for c in generator_config.edge_cases} - seen)
    assert missing == []


# --------------------------------------------------------------------------- #
# Output layout and contract (sections 3.1-3.3)
# --------------------------------------------------------------------------- #


def test_layout_has_every_load_and_separate_truth(small_run, generator_config):
    data_dir, summary = small_run
    expected = generator_config.preset("small").refresh_loads + 1
    assert summary.loads == expected
    for load_id in range(expected):
        assert (paths.load_dir(load_id, data_dir) / "events.parquet").is_file()
        assert (paths.load_dir(load_id, data_dir) / "manifest.json").is_file()
        assert (paths.truth_dir(load_id, data_dir) / "truth.parquet").is_file()
    assert not list((data_dir / "loads").rglob("truth*"))


def test_schemas_match_design(small_run):
    data_dir, _ = small_run
    events = pq.read_schema(paths.load_dir(1, data_dir) / "events.parquet")
    truth = pq.read_schema(paths.truth_dir(1, data_dir) / "truth.parquet")
    assert events.equals(EVENT_SCHEMA)
    assert truth.equals(TRUTH_SCHEMA)
    assert "true_person_id" not in events.names


def test_initial_load_inserts_exactly_the_preset_size(small_run, generator_config):
    data_dir, _ = small_run
    events = read_loads(data_dir, "loads", "events.parquet")[0]
    inserts = {e["event_id"] for e in events if e["op"] == "I"}
    assert len(inserts) == generator_config.preset("small").initial_records


def test_refresh_plans_follow_preset_and_mix(generator_config):
    ctx = make_context(generator_config)
    preset = generator_config.preset("medium")
    for ops in plan_refresh_ops(ctx, preset, stream(1, "load-sizes")):
        total = ops.inserts + ops.updates + ops.deletes
        assert preset.refresh_records_min <= total <= preset.refresh_records_max
        assert ops.inserts == round(total * 0.70)
        assert ops.updates == round(total * 0.25)


def test_every_event_key_has_truth_and_vice_versa(small_run):
    data_dir, _ = small_run
    events = read_loads(data_dir, "loads", "events.parquet")
    truth = read_loads(data_dir, "truth", "truth.parquet")
    event_keys = {(e["source_system"], e["source_record_id"]) for es in events.values() for e in es}
    truth_keys = {(t["source_system"], t["source_record_id"]) for ts in truth.values() for t in ts}
    assert event_keys == truth_keys


def test_sequence_numbers_identify_events(boosted_events):
    """A (key, sequence_no) pair always means the same event, even when redelivered."""
    seen: dict[tuple, str] = {}
    for events in boosted_events.values():
        for e in events:
            key = (e["source_system"], e["source_record_id"], e["sequence_no"])
            assert seen.setdefault(key, e["event_id"]) == e["event_id"]


def test_deletes_carry_only_keys(boosted_events):
    for events in boosted_events.values():
        for e in events:
            if e["op"] == "D":
                assert all(e[c] is None for c in ("first_name", "last_name", "dob", "ssn"))


def test_records_per_person_average_is_about_1_4(small_run):
    _, summary = small_run
    assert 1.25 <= summary.records / summary.people <= 1.65


# --------------------------------------------------------------------------- #
# Reproducibility (section 3.5)
# --------------------------------------------------------------------------- #


def _generate_in_subprocess(data_dir, seed: int, hash_seed: str) -> None:
    env = {**os.environ, "PYTHONHASHSEED": hash_seed}
    subprocess.run(
        [sys.executable, "-m", "pipeline", "generate", "--preset", "small",
         "--seed", str(seed), "--data-dir", str(data_dir)],
        check=True, env=env, capture_output=True, cwd=paths.REPO_ROOT,
    )  # fmt: skip


def test_same_seed_gives_byte_identical_files_across_processes(tmp_path):
    """Different PYTHONHASHSEED values prove no output depends on set/dict hashing."""
    _generate_in_subprocess(tmp_path / "a", 123, "1")
    _generate_in_subprocess(tmp_path / "b", 123, "2")
    first, second = file_hashes(tmp_path / "a"), file_hashes(tmp_path / "b")
    assert len(first) > 30
    assert first == second


def test_different_seed_gives_different_data(tmp_path, small_run):
    data_dir, _ = small_run
    _generate_in_subprocess(tmp_path / "c", 999, "0")
    assert file_hashes(tmp_path / "c") != file_hashes(data_dir)


# --------------------------------------------------------------------------- #
# Pipeline edge cases are visible in the events
# --------------------------------------------------------------------------- #


def test_duplicate_delivery_repeats_event_ids(boosted_events):
    counts = defaultdict(int)
    for events in boosted_events.values():
        for e in events:
            counts[e["event_id"]] += 1
    assert any(n > 1 for n in counts.values())


def test_out_of_order_delivers_older_sequence_later(boosted_events):
    max_seen: dict[tuple, int] = {}
    found = False
    for load in sorted(boosted_events):
        for e in boosted_events[load]:
            key = (e["source_system"], e["source_record_id"])
            if e["op"] == "U" and e["sequence_no"] < max_seen.get(key, 0):
                found = True
        for e in boosted_events[load]:
            key = (e["source_system"], e["source_record_id"])
            max_seen[key] = max(max_seen.get(key, 0), e["sequence_no"])
    assert found


def test_late_update_after_delete_has_older_sequence(boosted_events):
    deleted_at: dict[tuple, int] = {}
    found = False
    for load in sorted(boosted_events):
        for e in boosted_events[load]:
            key = (e["source_system"], e["source_record_id"])
            if e["op"] == "D":
                deleted_at[key] = e["sequence_no"]
            elif e["op"] == "U" and key in deleted_at and e["sequence_no"] < deleted_at[key]:
                found = True
    assert found


def test_column_shift_moves_values_right(boosted_events):
    shifted = [
        e
        for events in boosted_events.values()
        for e in events
        if e["op"] != "D" and e["first_name"] == "" and any(c.isalpha() for c in e["dob"] or "")
    ]
    assert shifted


def test_encoding_corruption_produces_mojibake(boosted_events):
    names = [
        (e["first_name"] or "") + (e["last_name"] or "")
        for es in boosted_events.values()
        for e in es
    ]
    assert any("Ã" in n for n in names)


def test_source_id_reuse_maps_one_key_to_two_people(boosted_truth):
    people = defaultdict(set)
    for rows in boosted_truth.values():
        for row in rows:
            people[(row["source_system"], row["source_record_id"])].add(row["true_person_id"])
    assert any(len(p) > 1 for p in people.values())


def test_records_are_rendered_in_their_source_format(small_run):
    data_dir, _ = small_run
    events = read_loads(data_dir, "loads", "events.parquet")[0]
    by_source = defaultdict(list)
    for e in events:
        by_source[e["source_system"]].append(e)
    assert any(e["ssn"].count("-") == 2 for e in by_source["EHR_A"])
    assert any(e["dob"].count("/") == 2 for e in by_source["CLAIMS_B"])
    assert all(e["first_name"] == e["first_name"].upper() for e in by_source["CLAIMS_B"])


# --------------------------------------------------------------------------- #
# Population semantics
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="module")
def boosted_population(generator_config):
    ctx = make_context(boosted(generator_config))
    population = build_population(ctx, [1500] + [40] * 12, stream(5, "population"))
    schedule_life_events(ctx, population.persons, stream(5, "timeline"))
    return ctx, population.persons


def _with_tag(persons, code):
    return [p for p in persons if code in p.tags]


def test_twins_share_dob_and_last_name_but_not_first_name_or_ssn(boosted_population):
    _, persons = boosted_population
    groups = defaultdict(list)
    for p in _with_tag(persons, "TWINS"):
        groups[(p.identity.last, p.identity.dob)].append(p)
    pairs = [g for g in groups.values() if len(g) == 2]
    assert pairs
    for a, b in pairs:
        assert a.identity.first != b.identity.first
        assert a.identity.ssn != b.identity.ssn


def test_jr_is_at_least_twenty_years_younger_with_same_name(boosted_population):
    _, persons = boosted_population
    sons = [p for p in _with_tag(persons, "JR_SR") if p.identity.suffix == "JR"]
    assert sons
    by_name = defaultdict(list)
    for p in _with_tag(persons, "JR_SR"):
        by_name[(p.identity.first, p.identity.last)].append(p)
    for son in sons:
        fathers = [
            p
            for p in by_name[(son.identity.first, son.identity.last)]
            if p is not son and p.identity.suffix != "JR"
        ]
        assert any((son.identity.dob - f.identity.dob).days >= 19 * 365 for f in fathers)


def test_newborn_carries_parent_ssn(boosted_population):
    _, persons = boosted_population
    tagged = _with_tag(persons, "NEWBORN_PARENT_SSN")
    newborns = [p for p in tagged if p.is_newborn]
    assert newborns
    by_ssn = defaultdict(list)
    for p in tagged:
        by_ssn[p.identity.ssn].append(p)
    for baby in newborns:
        parents = [p for p in by_ssn[baby.identity.ssn] if not p.is_newborn]
        assert parents
        assert all((baby.identity.dob - p.identity.dob).days > 12 * 365 for p in parents)


def test_fraudsters_share_the_victims_ssn(boosted_population):
    _, persons = boosted_population
    by_ssn = defaultdict(list)
    for p in _with_tag(persons, "SSN_SHARED_MANY"):
        by_ssn[p.identity.ssn].append(p)
    assert any(len(group) >= 4 for group in by_ssn.values())


def test_newborns_are_born_shortly_before_their_first_record(boosted_population):
    ctx, persons = boosted_population
    newborns = [p for p in persons if p.is_newborn and p.record_loads]
    assert newborns
    for p in newborns:
        # The true DOB: DOB_CORRECTED people start with a wrong one.
        true_dob = p.identity_at(ctx.last_load).dob
        first_record = ctx.load_dates[p.first_load]
        assert timedelta(0) <= first_record - true_dob <= timedelta(days=366)


def test_nobody_is_born_in_the_future(boosted_population):
    ctx, persons = boosted_population
    for p in persons:
        assert p.identity.dob <= ctx.load_dates[-1]
        assert p.identity_at(ctx.last_load).dob <= ctx.load_dates[-1]


def test_life_events_are_ordered_and_stack(boosted_population):
    _, persons = boosted_population
    for p in persons:
        loads = [c.load_id for c in p.changes]
        assert loads == sorted(loads)
    assert any(len({c.code for c in p.changes}) >= 2 for p in persons)


def test_frequent_record_case_rate_is_close_to_config(small_run, generator_config):
    """Sanity check on the real config: NAME_TYPO lands near its 4% rate."""
    data_dir, _ = small_run
    latest = _latest_truth(read_loads(data_dir, "truth", "truth.parquet"))
    share = sum("NAME_TYPO" in r["edge_case_tags"] for r in latest.values()) / len(latest)
    assert 0.015 <= share <= 0.07


# --------------------------------------------------------------------------- #
# Reference and name data used by the generator
# --------------------------------------------------------------------------- #


def test_generator_placeholders_exist_in_reference(reference_data):
    names = reference_data.placeholder_values("name")
    used = set(FIRST_NAME_PLACEHOLDERS) | set(LAST_NAME_PLACEHOLDERS)
    used |= {n for pair in FULL_NAME_PLACEHOLDERS for n in pair} | {"BABY BOY", "BABY GIRL"}
    assert used <= names


def test_phonetic_variants_share_a_double_metaphone_code():
    with (DATA_DIR / "phonetic_variants.csv").open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows
    for row in rows:
        a = {c for c in doublemetaphone(row["name"].upper()) if c}
        b = {c for c in doublemetaphone(row["variant"].upper()) if c}
        assert a & b, (row["name"], row["variant"])


def test_every_configured_culture_has_names(generator_config):
    pools = NamePools()
    rng = stream(0, "names-test")
    for culture in generator_config.population.culture_mix:
        for gender in ("F", "M"):
            assert pools.first_name(rng, culture, gender)
        assert pools.last_name(rng, culture)


# --------------------------------------------------------------------------- #
# Generated documentation (section 3.7)
# --------------------------------------------------------------------------- #


def test_data_generation_doc_is_up_to_date(generator_config):
    committed = DOC_PATH.read_text(encoding="utf-8")
    assert committed == render_markdown(generator_config), (
        "docs/DATA_GENERATION.md is stale; run `python -m pipeline docs`"
    )


def test_data_generation_doc_lists_every_code_with_its_rate(generator_config):
    text = render_markdown(generator_config)
    for case in generator_config.edge_cases:
        assert f"`{case.code}`" in text
