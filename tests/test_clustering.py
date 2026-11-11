"""Shared clustering and ID reconciliation (DESIGN.md sections 10 and 11)."""

from __future__ import annotations

import random

import pytest

from clustering.constrained import (
    CannotLink,
    MatchEdge,
    cluster_component,
    cluster_records,
    connected_components,
)
from clustering.ids import ALL_DELETED, MERGED, Retirement, reconcile
from pipeline.config import PersonIdFormat

FMT = PersonIdFormat(prefix="P", digits=9)


def pid(n: int) -> str:
    return FMT.format(n)


# --------------------------------------------------------------------------- #
# Clustering
# --------------------------------------------------------------------------- #


def test_components_follow_match_edges():
    edges = [MatchEdge("A", "B", 1), MatchEdge("C", "D", 2)]
    assert connected_components(["A", "B", "C", "D", "E"], edges) == {
        "A": "A",
        "B": "A",
        "C": "C",
        "D": "C",
        "E": "E",
    }


def test_matches_are_joined_transitively_without_conflicts():
    result = cluster_records("ABC", [MatchEdge("A", "B", 1), MatchEdge("B", "C", 3)], [])
    assert set(result.cluster_of.values()) == {"A"} and result.skipped == ()


def test_cannot_link_stops_a_transitive_join():
    """A-B and B-C match, but A and C are blocked: C stays out, and the skip is recorded."""
    edges = [MatchEdge("A", "B", 1), MatchEdge("B", "C", 2)]
    result = cluster_records("ABC", edges, [CannotLink("A", "C", "BLOCKED_TWINS")])
    assert result.cluster_of == {"A": "A", "B": "A", "C": "C"}
    (skip,) = result.skipped
    assert (skip.record_key_a, skip.record_key_b, skip.tier, skip.reason_code) == (
        "B",
        "C",
        2,
        "SKIPPED_TRANSITIVITY",
    )
    assert (skip.conflict_record_key_a, skip.conflict_record_key_b, skip.conflict_reason_code) == (
        "A",
        "C",
        "BLOCKED_TWINS",
    )


def test_stronger_tiers_are_joined_first():
    """B matches A at tier 3 and C at tier 1; A and C cannot link. The tier 1 join wins."""
    edges = [MatchEdge("A", "B", 3), MatchEdge("B", "C", 1)]
    result = cluster_records("ABC", edges, [CannotLink("A", "C", "BLOCKED_SUFFIX")])
    assert result.cluster_of == {"A": "A", "B": "B", "C": "B"}
    assert [(s.record_key_a, s.record_key_b) for s in result.skipped] == [("A", "B")]


def test_equal_tiers_are_ordered_by_record_keys():
    edges = [MatchEdge("B", "C", 1), MatchEdge("A", "B", 1)]
    result = cluster_records("ABC", edges, [CannotLink("A", "C", "BLOCKED_TWINS")])
    assert result.cluster_of["B"] == "A" and result.cluster_of["C"] == "C"


def test_unmatched_records_are_singletons_and_cluster_id_is_smallest_key():
    result = cluster_records(["X", "M", "Q"], [MatchEdge("X", "M", 1)], [])
    assert result.cluster_of == {"M": "M", "Q": "Q", "X": "M"}


def test_result_does_not_depend_on_input_order():
    rng = random.Random(7)
    keys = [f"K{i:03d}" for i in range(60)]
    edges = [MatchEdge(*sorted(rng.sample(keys, 2)), rng.randint(1, 4)) for _ in range(120)]
    edges = list({(e.record_key_a, e.record_key_b): e for e in edges}.values())
    links = [CannotLink(*sorted(rng.sample(keys, 2)), "BLOCKED_TWINS") for _ in range(25)]
    expected = cluster_records(keys, edges, links)
    for _ in range(5):
        rng.shuffle(keys), rng.shuffle(edges), rng.shuffle(links)
        assert cluster_records(keys, edges, links) == expected


def test_no_cluster_ever_contains_a_cannot_link_pair():
    rng = random.Random(11)
    keys = [f"K{i:03d}" for i in range(80)]
    edges = [MatchEdge(*sorted(rng.sample(keys, 2)), rng.randint(1, 4)) for _ in range(200)]
    edges = list({(e.record_key_a, e.record_key_b): e for e in edges}.values())
    links = [CannotLink(*sorted(rng.sample(keys, 2)), "BLOCKED_TWINS") for _ in range(40)]
    result = cluster_records(keys, edges, links)
    for link in links:
        assert result.cluster_of[link.record_key_a] != result.cluster_of[link.record_key_b]


def test_per_component_clustering_equals_global_clustering():
    """PySpark clusters each component separately; dbt clusters everything at once."""
    rng = random.Random(3)
    keys = [f"K{i:03d}" for i in range(50)]
    edges = [MatchEdge(*sorted(rng.sample(keys, 2)), rng.randint(1, 4)) for _ in range(60)]
    edges = list({(e.record_key_a, e.record_key_b): e for e in edges}.values())
    links = [CannotLink(*sorted(rng.sample(keys, 2)), "BLOCKED_TWINS") for _ in range(20)]
    whole = cluster_records(keys, edges, links)
    components = connected_components(keys, edges)
    combined = {}
    for component in set(components.values()):
        members = [k for k, c in components.items() if c == component]
        part = cluster_component(
            members,
            [e for e in edges if components[e.record_key_a] == component],
            [link for link in links if components[link.record_key_a] == component],
        )
        combined.update(part.cluster_of)
    assert combined == whole.cluster_of


# --------------------------------------------------------------------------- #
# ID reconciliation: one test per rule in section 11
# --------------------------------------------------------------------------- #


def test_first_load_assigns_new_ids_in_cluster_order():
    result = reconcile({"r2": "c2", "r1": "c1", "r3": "c1"}, {}, 1, FMT)
    assert result.person_of == {"r1": pid(1), "r2": pid(2), "r3": pid(1)}
    assert result.next_sequence == 3 and result.retirements == ()


def test_cluster_from_one_previous_id_keeps_it():
    result = reconcile({"r1": "c1", "r2": "c1"}, {"r1": pid(5), "r2": pid(5)}, 9, FMT)
    assert result.person_of == {"r1": pid(5), "r2": pid(5)} and result.new_ids == ()


def test_merge_keeps_the_oldest_id_and_retires_the_others():
    previous = {"r1": pid(7), "r2": pid(3), "r3": pid(9)}
    result = reconcile({"r1": "c", "r2": "c", "r3": "c"}, previous, 10, FMT)
    assert set(result.person_of.values()) == {pid(3)}
    assert result.retirements == (
        Retirement(pid(7), pid(3), MERGED),
        Retirement(pid(9), pid(3), MERGED),
    )


def test_split_keeps_the_id_in_the_larger_part_and_issues_a_new_one():
    previous = {"r1": pid(4), "r2": pid(4), "r3": pid(4)}
    result = reconcile({"r1": "a", "r2": "b", "r3": "b"}, previous, 10, FMT)
    assert result.person_of == {"r1": pid(10), "r2": pid(4), "r3": pid(4)}
    assert result.retirements == ()


def test_split_ties_go_to_the_smallest_cluster_id():
    result = reconcile({"r1": "b", "r2": "a"}, {"r1": pid(4), "r2": pid(4)}, 10, FMT)
    assert result.person_of == {"r1": pid(10), "r2": pid(4)}


def test_cluster_without_previous_records_gets_a_new_id():
    result = reconcile({"r1": "a", "new": "b"}, {"r1": pid(1)}, 2, FMT)
    assert result.person_of == {"new": pid(2), "r1": pid(1)}


def test_all_records_deleted_retires_the_id_without_successor():
    result = reconcile({"r1": "a"}, {"r1": pid(1), "gone": pid(2)}, 3, FMT)
    assert result.retirements == (Retirement(pid(2), None, ALL_DELETED),)


def test_retired_ids_are_never_reissued():
    previous = {"r1": pid(1), "r2": pid(2)}
    merged = reconcile({"r1": "a", "r2": "a"}, previous, 3, FMT)
    split = reconcile({"r1": "a", "r2": "b"}, merged.person_of, merged.next_sequence, FMT)
    assert split.person_of == {"r1": pid(1), "r2": pid(3)}  # not pid(2) again


def test_a_split_id_that_merges_elsewhere_competes_by_age():
    """P5's majority joins P2's cluster: P2 is older and survives; P5 is retired."""
    previous = {"a1": pid(2), "b1": pid(5), "b2": pid(5), "b3": pid(5)}
    result = reconcile({"a1": "x", "b1": "x", "b2": "x", "b3": "y"}, previous, 6, FMT)
    assert result.person_of == {"a1": pid(2), "b1": pid(2), "b2": pid(2), "b3": pid(6)}
    assert result.retirements == (Retirement(pid(5), pid(2), MERGED),)


def test_reconciling_the_same_clusters_again_changes_nothing():
    first = reconcile({"r1": "a", "r2": "a", "r3": "b"}, {}, 1, FMT)
    again = reconcile({"r1": "a", "r2": "a", "r3": "b"}, first.person_of, first.next_sequence, FMT)
    assert again.person_of == first.person_of
    assert again.retirements == () and again.new_ids == ()


def test_cluster_ids_may_change_without_changing_person_ids():
    """Cluster ids are recomputed each load; only membership matters."""
    first = reconcile({"r1": "r1", "r2": "r1"}, {}, 1, FMT)
    again = reconcile({"r1": "r0", "r2": "r0", "r0": "r0"}, first.person_of, 2, FMT)
    assert again.person_of["r1"] == again.person_of["r2"] == first.person_of["r1"]


def test_locality_a_change_only_touches_the_affected_clusters():
    clusters = {f"r{i:03d}": f"c{i // 3:03d}" for i in range(300)}
    first = reconcile(clusters, {}, 1, FMT)
    changed = dict(clusters)
    changed["r010"] = "c020"  # one record moves from one person to another
    changed["new"] = "c099"
    del changed["r030"]
    second = reconcile(changed, first.person_of, first.next_sequence, FMT)
    affected = {"c003", "c020", "c099", "c010"}
    for uid, person_id in second.person_of.items():
        if changed[uid] not in affected:
            assert person_id == first.person_of[uid]
    kept = [u for u in second.person_of if u in first.person_of]
    assert sum(first.person_of[u] != second.person_of[u] for u in kept) == 1


@pytest.mark.parametrize("bad", ["X000000001", "P0000000AB"])
def test_foreign_ids_are_rejected(bad):
    with pytest.raises(ValueError):
        reconcile({"r1": "a"}, {"r1": bad}, 1, FMT)


def test_matching_rules_example_suffix_conflict_stops_a_chain():
    """The clustering example in docs/MATCHING_RULES.md section 6, with real pair decisions."""
    from matching_helpers import decide_records, record

    from standardization.blocking import record_key

    base = {"first_name": "John", "ssn": "234-56-7801", "gender": "M"}
    a = record(source_record_id="A", last_name="Smith Sr", dob="1980-05-08", **base)
    b = record(source_record_id="B", last_name="Smith", dob="1980-05-06", **base)
    c = record(source_record_id="C", last_name="Smith Jr", dob="1980-05-06", **base)
    decisions = {(x, y): decide_records(x, y) for x, y in ((a, b), (b, c), (a, c))}
    assert [d.reason_code for d in decisions.values()] == [
        "TIER_3_DOB_NEAR",
        "TIER_1_EXACT_IDS",
        "BLOCKED_SUFFIX",
    ]
    edges = [
        MatchEdge(record_key(x), record_key(y), d.tier)
        for (x, y), d in decisions.items()
        if d.is_match
    ]
    links = [
        CannotLink(record_key(x), record_key(y), d.reason_code)
        for (x, y), d in decisions.items()
        if d.cannot_link
    ]
    result = cluster_records([record_key(r) for r in (a, b, c)], edges, links)
    assert result.cluster_of == {"EHR_A|A": "EHR_A|A", "EHR_A|B": "EHR_A|B", "EHR_A|C": "EHR_A|B"}
    (skip,) = result.skipped
    assert (skip.record_key_a, skip.record_key_b) == ("EHR_A|A", "EHR_A|B")
    assert (skip.conflict_record_key_a, skip.conflict_record_key_b) == ("EHR_A|A", "EHR_A|C")
