"""Scenario tests: one hand-built case per edge case code (DESIGN.md sections 3.7 and 15).

Each test asserts the catalog's expected outcome on the Python reference
implementation. The dbt (phase 5) and PySpark (phase 7) implementations must
reproduce these decisions. Codes about event delivery are covered by the
ingestion tests listed in ``INGESTION_SCENARIOS``.

Unless a test says otherwise, the profile is conservative and the pair starts
from ``BASE`` in ``matching_helpers``: CATHERINE MILLER, 1980-05-06,
234-56-7801, F.
"""

from __future__ import annotations

from datetime import date

import pytest
import test_ingestion
from matching_helpers import decide_records, pair, record

from generator.catalog import implementations

COVERED: set[str] = set()


def covers(*codes: str):
    """Mark a test as the scenario for these catalog codes."""
    COVERED.update(codes)
    return lambda test: test


def assert_outcome(decision, reason: str, is_match: bool) -> None:
    assert (decision.reason_code, decision.is_match) == (reason, is_match), decision


# --------------------------------------------------------------------------- #
# SSN
# --------------------------------------------------------------------------- #


@covers("SSN_TYPO_SUB")
def test_ssn_typo_substitution():
    assert_outcome(pair({}, {"ssn": "234-56-7811"}), "TIER_2_SSN_NEAR", True)


@covers("SSN_TYPO_TRANSPOSE")
def test_ssn_typo_transposition():
    assert_outcome(pair({}, {"ssn": "234-56-7810"}), "TIER_2_SSN_NEAR", True)


@covers("SSN_NUMERIC_CORRUPT")
@pytest.mark.parametrize("corrupted", ["12345678", "12345678.0", "1.23456780E+07"])
def test_ssn_numeric_corruption_is_fixed(corrupted):
    assert_outcome(pair({"ssn": "012-34-5678"}, {"ssn": corrupted}), "TIER_1_EXACT_IDS", True)


@covers("SSN_PLACEHOLDER")
@pytest.mark.parametrize("placeholder", ["000-00-0000", "123-45-6789", "999-99-9999", "222222222"])
def test_ssn_placeholder_is_missing(placeholder):
    decision = pair({}, {"ssn": placeholder})
    assert decision.comparison.ssn_level == "MISSING"
    assert_outcome(decision, "TIER_4_NO_SSN", False)


@covers("SSN_TEMP_ITIN")
def test_temporary_itin_is_missing():
    decision = pair({}, {"ssn": "912-70-1234"})
    assert decision.comparison.ssn_level == "MISSING"
    assert_outcome(decision, "TIER_4_NO_SSN", False)


@covers("SSN_DELAYED")
def test_delayed_ssn_matches_once_added():
    assert_outcome(pair({}, {"ssn": ""}), "TIER_4_NO_SSN", False)
    assert_outcome(pair({}, {}), "TIER_1_EXACT_IDS", True)  # the same record, updated


@covers("SSN_CORRECTED")
def test_corrected_ssn_rematches():
    assert_outcome(pair({}, {"ssn": "345-67-8902"}), "NO_MATCH_INSUFFICIENT", False)
    assert_outcome(pair({}, {"ssn": "234-56-7801"}), "TIER_1_EXACT_IDS", True)


# --------------------------------------------------------------------------- #
# DOB
# --------------------------------------------------------------------------- #


@covers("DOB_TYPO_DIGIT")
def test_dob_one_digit_typo():
    assert_outcome(pair({}, {"dob": "1980-05-08"}), "TIER_3_DOB_NEAR", True)


@covers("DOB_MONTH_DAY_SWAP")
def test_dob_month_day_swap():
    assert_outcome(pair({}, {"dob": "06/05/1980"}), "TIER_3_DOB_NEAR", True)


@covers("DOB_YEAR_OFF_ONE")
def test_dob_year_off_by_one():
    assert_outcome(pair({}, {"dob": "1981-05-06"}), "TIER_3_DOB_NEAR", True)


@covers("DOB_LEAP_DAY")
@pytest.mark.parametrize("recorded", ["1984-02-28", "1984-03-01"])
def test_dob_leap_day_shift(recorded):
    assert_outcome(pair({"dob": "1984-02-29"}, {"dob": recorded}), "TIER_3_DOB_NEAR", True)


@covers("DOB_PLACEHOLDER")
def test_dob_placeholder_is_missing():
    decision = pair({}, {"dob": "1900-01-01"})
    assert decision.comparison.dob_level == "MISSING"
    assert_outcome(decision, "NO_MATCH_INSUFFICIENT", False)


@covers("DOB_JAN_FIRST")
def test_jan_first_is_kept_but_weak():
    jan1 = {"dob": "1980-01-01"}
    assert_outcome(pair(jan1, jan1), "TIER_1_EXACT_IDS", True)
    assert_outcome(pair(jan1, {**jan1, "ssn": "234-56-7811"}), "NO_MATCH_INSUFFICIENT", False)
    assert_outcome(
        pair(jan1, {**jan1, "ssn": ""}, profile="aggressive"), "NO_MATCH_INSUFFICIENT", False
    )


@covers("DOB_TWO_DIGIT_YEAR")
def test_two_digit_year_read_as_future_is_missing():
    decision = pair({}, {"dob": "05/06/80"})
    assert decision.comparison.dob_level == "MISSING"
    assert record(dob="05/06/80").dob_null_reason == "FUTURE"


@covers("DOB_YEAR_ONLY")
def test_year_only_dob_is_missing():
    assert pair({}, {"dob": "1980"}).comparison.dob_level == "MISSING"


@covers("DOB_CORRECTED")
def test_corrected_dob_rematches():
    assert_outcome(pair({}, {"dob": "1975-11-20"}), "NO_MATCH_INSUFFICIENT", False)
    assert_outcome(pair({}, {"dob": "1980-05-06"}), "TIER_1_EXACT_IDS", True)


# --------------------------------------------------------------------------- #
# Names (SSN NEAR, so both names must be STRONG: Tier 2)
# --------------------------------------------------------------------------- #

NEAR_SSN = {"ssn": "234-56-7811"}


def name_pair(a: dict, b: dict):
    return pair(a, {**b, **NEAR_SSN})


@covers("NAME_TYPO")
def test_name_typo():
    decision = name_pair({}, {"first_name": "Catherime"})
    assert decision.comparison.first_method == "FUZZY"
    assert_outcome(decision, "TIER_2_SSN_NEAR", True)


@covers("NAME_NICKNAME")
@pytest.mark.parametrize(
    ("name", "nickname"), [("David", "Dave"), ("Robert", "Bob"), ("Margaret", "Peggy")]
)
def test_nickname(name, nickname):
    decision = name_pair({"first_name": name}, {"first_name": nickname})
    assert decision.comparison.first_method == "VARIANT"
    assert_outcome(decision, "TIER_2_SSN_NEAR", True)


@covers("NAME_PHONETIC")
def test_phonetic_variant():
    decision = name_pair({}, {"first_name": "Katherine", "last_name": "Millar"})
    assert decision.comparison.first_method == "PHONETIC"
    assert_outcome(decision, "TIER_2_SSN_NEAR", True)
    assert (
        name_pair({"last_name": "Smith"}, {"last_name": "Smyth"}).comparison.last_method
        == "PHONETIC"
    )


@covers("NAME_TRANSLITERATION")
@pytest.mark.parametrize("variant", ["Muhammad", "Mohamed"])
def test_transliteration(variant):
    decision = name_pair({"first_name": "Mohammed"}, {"first_name": variant})
    assert decision.comparison.first_method == "VARIANT"
    assert_outcome(decision, "TIER_2_SSN_NEAR", True)


@covers("NAME_SWAPPED")
def test_swapped_names():
    decision = pair({}, {"first_name": "Miller", "last_name": "Catherine"})
    assert decision.comparison.names_swapped
    assert_outcome(decision, "TIER_1_EXACT_IDS", True)
    assert_outcome(
        name_pair({}, {"first_name": "Miller", "last_name": "Catherine"}), "TIER_2_SSN_NEAR", True
    )


@covers("NAME_FAMILY_FIRST")
def test_family_name_first_source():
    decision = pair(
        {}, {"source_system": "CLAIMS_B", "first_name": "MILLER", "last_name": "CATHERINE"}
    )
    assert decision.comparison.names_swapped
    assert_outcome(decision, "TIER_1_EXACT_IDS", True)


@covers("NAME_MIDDLE_IN_FIRST")
def test_middle_name_in_first_name():
    assert_outcome(
        name_pair({"first_name": "Mary"}, {"first_name": "Mary Ann"}), "TIER_2_SSN_NEAR", True
    )


@covers("NAME_CONCATENATED")
def test_concatenated_name():
    decision = name_pair({"first_name": "Mary Ann"}, {"first_name": "MaryAnn"})
    assert decision.comparison.first_method == "EXACT"
    assert_outcome(decision, "TIER_2_SSN_NEAR", True)


@covers("NAME_INITIAL")
def test_initial_is_weak():
    initial = {"first_name": "C."}
    assert pair({}, initial).comparison.first_level == "WEAK"
    assert_outcome(pair({}, initial), "TIER_1_EXACT_IDS", True)  # one strong name is enough
    assert_outcome(name_pair({}, initial), "NO_MATCH_INSUFFICIENT", False)


@covers("NAME_TRUNCATED")
def test_truncated_name_prefix_rule():
    decision = name_pair({"first_name": "Maximilian"}, {"first_name": "MAXIMI"})
    assert decision.comparison.first_method == "PREFIX"
    assert_outcome(decision, "TIER_2_SSN_NEAR", True)
    assert_outcome(
        name_pair({"first_name": "Maximilian"}, {"first_name": "MAXIM"}),
        "NO_MATCH_INSUFFICIENT",
        False,
    )


@covers("NAME_COMPOUND_SURNAME")
def test_compound_surname_token_match():
    assert_outcome(
        name_pair({"last_name": "García López"}, {"last_name": "Lopez"}), "TIER_2_SSN_NEAR", True
    )


@covers("NAME_PARTICLE")
@pytest.mark.parametrize("other", ["Delacruz", "Cruz", "DE LA CRUZ"])
def test_particle_stripped_comparison(other):
    assert_outcome(
        name_pair({"last_name": "De la Cruz"}, {"last_name": other}), "TIER_2_SSN_NEAR", True
    )


@covers("NAME_UNICODE")
@pytest.mark.parametrize(("a", "b"), [("José", "Jose"), ("José", "José"), ("Søren", "SOREN")])
def test_unicode_names(a, b):
    decision = name_pair({"first_name": a}, {"first_name": b})
    assert decision.comparison.first_method == "EXACT"
    assert_outcome(decision, "TIER_2_SSN_NEAR", True)


@covers("NAME_SUFFIX_IN_LAST")
@pytest.mark.parametrize("written", ["Miller Jr.", "Miller, Jr", "MILLER JUNIOR"])
def test_suffix_in_last_name(written):
    r = record(last_name=written)
    assert (r.last_name, r.suffix) == ("MILLER", "JR")
    assert_outcome(
        pair({"last_name": "Miller Jr"}, {"last_name": written}), "TIER_1_EXACT_IDS", True
    )


@covers("NAME_PLACEHOLDER")
@pytest.mark.parametrize(
    ("first", "last"),
    [
        ("Baby Boy", "Miller"),
        ("Infant", "Miller"),
        ("Unknown", "Miller"),
        ("FNU", "Miller"),
        ("Catherine", "LNU"),
        ("Asdf", "Miller"),
        ("Test", "User"),
    ],
)
def test_placeholder_name_is_missing(first, last):
    decision = pair({}, {"first_name": first, "last_name": last})
    c = decision.comparison
    assert "MISSING" in (c.first_method, c.last_method)
    expected = "NO_MATCH_INSUFFICIENT" if (first, last) == ("Test", "User") else "TIER_1_EXACT_IDS"
    assert_outcome(decision, expected, expected != "NO_MATCH_INSUFFICIENT")


@covers("NAME_MONONYM")
def test_mononym():
    # One legal name, recorded once with LNU and once repeated in both fields.
    lnu = record(first_name="Madonna", last_name="LNU")
    repeated = record(first_name="Madonna", last_name="Madonna")
    assert lnu.last_name is None
    assert_outcome(decide_records(lnu, repeated), "TIER_1_EXACT_IDS", True)
    # Without the SSN, one strong name is not enough: documented limitation.
    no_ssn = record(first_name="Madonna", last_name="LNU", ssn="")
    assert_outcome(decide_records(no_ssn, repeated, "aggressive"), "NO_MATCH_INSUFFICIENT", False)


# --------------------------------------------------------------------------- #
# Life events (SSN and DOB exact: Tier 1 needs one strong name)
# --------------------------------------------------------------------------- #


@covers("MARRIAGE", "DIVORCE_RESTORE", "ADOPTION_NAME_CHANGE")
def test_last_name_change():
    assert_outcome(pair({}, {"last_name": "Okafor"}), "TIER_1_EXACT_IDS", True)
    assert_outcome(pair({"last_name": "Okafor"}, {}), "TIER_1_EXACT_IDS", True)


@covers("LEGAL_FIRST_NAME_CHANGE")
def test_first_name_change_with_gender():
    """Same SSN, DOB and last name: a name change, not twins (D-036)."""
    assert_outcome(pair({}, {"first_name": "Daniel", "gender": "M"}), "TIER_1_EXACT_IDS", True)


def test_twins_on_two_different_ssns_are_blocked():
    a = record(first_name="Emma", ssn="345-67-8901", dob="2010-02-03")
    b = record(first_name="Olivia", ssn="456-78-9012", dob="2010-02-03")
    assert_outcome(decide_records(a, b), "BLOCKED_TWINS", False)


@covers("GENDER_CHANGE_OR_ERROR")
@pytest.mark.parametrize("gender", ["M", "X", "Q", "?", ""])
def test_gender_is_ignored(gender):
    assert_outcome(pair({}, {"gender": gender}), "TIER_1_EXACT_IDS", True)


@covers("FULL_NAME_CHANGE")
def test_full_name_change_is_a_known_limitation():
    assert_outcome(
        pair({}, {"first_name": "Daniel", "last_name": "Okafor"}), "NO_MATCH_INSUFFICIENT", False
    )


@covers("SSN_REPLACEMENT")
def test_ssn_replacement_is_a_known_limitation():
    assert_outcome(pair({}, {"ssn": "345-67-8902"}), "NO_MATCH_INSUFFICIENT", False)


# --------------------------------------------------------------------------- #
# Relationships and fraud
# --------------------------------------------------------------------------- #


@covers("TWINS")
def test_twins_are_blocked():
    a = record(first_name="Emma", ssn="345-67-8901", dob="2010-02-03")
    b = record(first_name="Olivia", ssn="345-67-8902", dob="2010-02-03")
    assert_outcome(decide_records(a, b), "BLOCKED_TWINS", False)
    assert decide_records(a, b).cannot_link


def test_twins_with_phonetically_similar_names_match_as_designed():
    """Known weakness kept on purpose (D-034): JOHN/JANE share Double Metaphone JN.

    Measured in phase 6 before any rule change.
    """
    a = record(first_name="John", ssn="345-67-8901", dob="2010-02-03", gender="M")
    b = record(first_name="Jane", ssn="345-67-8902", dob="2010-02-03", gender="F")
    assert_outcome(decide_records(a, b), "TIER_2_SSN_NEAR", True)


def test_twins_blocker_needs_both_first_names():
    """A placeholder first name is missing, not different (D-035)."""
    a = record(first_name="Baby Girl", ssn="345-67-8901", dob="2010-02-03")
    b = record(first_name="Emma", ssn="345-67-8902", dob="2010-02-03")
    assert_outcome(decide_records(a, b), "NO_MATCH_INSUFFICIENT", False)


@covers("MULTIPLES")
def test_multiples_are_blocked_pairwise():
    births = [
        record(first_name=name, ssn=f"345-67-890{i}", dob="2012-07-08")
        for i, name in enumerate(["Liam", "Noah", "Oliver"], start=1)
    ]
    for i, a in enumerate(births):
        for b in births[i + 1 :]:
            assert_outcome(decide_records(a, b), "BLOCKED_TWINS", False)


@covers("NEWBORN_PARENT_SSN")
def test_newborn_with_parent_ssn():
    parent = record(dob="1990-04-05")
    baby = record(first_name="Baby Girl", dob="2025-03-01")
    decision = decide_records(parent, baby)
    assert_outcome(decision, "BLOCKED_GENERATIONAL", False)


@covers("NEWBORN_PLACEHOLDER")
def test_newborn_placeholder_name_later_replaced():
    placeholder = record(
        first_name="Baby Boy", last_name="Smith", dob="2025-03-01", ssn="456-78-9012"
    )
    named = record(first_name="Liam", last_name="Smith", dob="2025-03-01", ssn="456-78-9012")
    assert_outcome(decide_records(placeholder, named), "TIER_1_EXACT_IDS", True)


def test_newborn_placeholder_against_adult_without_shared_ssn_is_blocked():
    baby = record(first_name="Baby Boy", last_name="Smith", dob="2025-03-01", ssn="")
    adult = record(first_name="John", last_name="Smith", dob="1990-03-01", ssn="456-78-9012")
    decision = decide_records(baby, adult)
    assert_outcome(decision, "BLOCKED_GENERATIONAL", False)
    # A placeholder name on an older record is not a newborn.
    toddler = record(first_name="Baby Boy", last_name="Smith", dob="2022-03-01", ssn="")
    assert decide_records(toddler, adult).blockers == ()


@covers("JR_SR")
def test_father_and_son():
    father = record(first_name="John", last_name="Smith Sr", dob="1970-03-04", ssn="456-78-9012")
    son = record(first_name="John", last_name="Smith Jr", dob="2000-08-09", ssn="456-78-9012")
    decision = decide_records(father, son)
    assert_outcome(decision, "BLOCKED_GENERATIONAL", False)
    assert decision.blockers == ("BLOCKED_GENERATIONAL", "BLOCKED_SUFFIX")
    son_own_ssn = record(
        first_name="John", last_name="Smith Jr", dob="2000-08-09", ssn="567-89-0123"
    )
    assert_outcome(decide_records(father, son_own_ssn), "BLOCKED_SUFFIX", False)
    son_no_suffix = record(
        first_name="John", last_name="Smith", dob="2000-08-09", ssn="456-78-9012"
    )
    assert_outcome(decide_records(father, son_no_suffix), "BLOCKED_GENERATIONAL", False)


@covers("COMMON_NAME_SAME_DOB")
def test_common_name_same_dob_without_ssn():
    a = {"first_name": "James", "last_name": "Smith", "ssn": ""}
    b = {"first_name": "James", "last_name": "Smith", "ssn": ""}
    assert_outcome(pair(a, b, "conservative"), "TIER_4_NO_SSN", False)
    assert_outcome(pair(a, b, "balanced"), "TIER_4_NO_SSN", False)
    assert_outcome(pair(a, b, "aggressive"), "TIER_4_NO_SSN", True)


@covers("IDENTICAL_STRANGERS", "FULL_IDENTITY_COPY")
def test_identical_values_are_a_known_limitation():
    assert_outcome(pair(), "TIER_1_EXACT_IDS", True)


@covers("SSN_SHARED_FRAUD")
def test_shared_ssn_fraud():
    fraud = {"first_name": "Derek", "last_name": "Novak", "dob": "1972-09-14"}
    assert_outcome(pair({}, fraud), "BLOCKED_SSN_CONFLICT", False)


@covers("SSN_SHARED_MANY")
def test_many_fraudsters_on_one_ssn():
    victim = record()
    fraudsters = [
        record(first_name=first, last_name=last, dob=dob)
        for first, last, dob in [
            ("Derek", "Novak", "1972-09-14"),
            ("Wei", "Zhang", "1991-12-02"),
            ("Omar", "Haddad", "1965-07-30"),
        ]
    ]
    everyone = [victim, *fraudsters]
    for i, a in enumerate(everyone):
        for b in everyone[i + 1 :]:
            decision = decide_records(a, b)
            assert not decision.is_match and "BLOCKED_SSN_CONFLICT" in decision.blockers


@covers("SYNTHETIC_IDENTITY")
def test_synthetic_identity():
    ssn_owner = record()
    name_owner = record(first_name="Derek", last_name="Novak", dob="1972-09-14", ssn="567-89-0123")
    synthetic = record(first_name="Derek", last_name="Novak", dob="1972-09-14")
    assert_outcome(decide_records(ssn_owner, synthetic), "BLOCKED_SSN_CONFLICT", False)
    assert_outcome(decide_records(name_owner, synthetic), "NO_MATCH_INSUFFICIENT", False)


@covers("NEAR_MATCH_ATTACK")
def test_near_match_attack_exposure():
    """Measured, not prevented: one changed character still matches if all else agrees."""
    assert_outcome(pair({}, {"ssn": "234-56-7802"}), "TIER_2_SSN_NEAR", True)
    # Two imperfect fields are never an auto-match.
    assert_outcome(
        pair({}, {"ssn": "234-56-7802", "dob": "1980-05-07"}), "NO_MATCH_INSUFFICIENT", False
    )


# --------------------------------------------------------------------------- #
# Source changes
# --------------------------------------------------------------------------- #


@covers("MIGRATION_REFORMAT")
def test_migration_reformat():
    migrated = {
        "first_name": "CATHERINE",
        "last_name": "MILLER",
        "dob": "19800506",
        "ssn": "234567801",
        "gender": "U",
    }
    assert_outcome(pair({}, migrated), "TIER_1_EXACT_IDS", True)
    defaulted = {**migrated, "ssn": "000000000", "first_name": "CATHERINE"[:10]}
    assert_outcome(pair({}, defaulted), "TIER_4_NO_SSN", False)
    truncated = {**migrated, "first_name": "MAXIMILIAN"[:6]}
    assert_outcome(name_pair({"first_name": "Maximilian"}, truncated), "TIER_2_SSN_NEAR", True)


@covers("SOURCE_ID_CHANGE")
def test_new_source_id_matches_on_attributes():
    old, new = record(source_record_id="EA00000001"), record(source_record_id="EA00009999")
    assert_outcome(decide_records(old, new), "TIER_1_EXACT_IDS", True)


# --------------------------------------------------------------------------- #
# Coverage of the catalog
# --------------------------------------------------------------------------- #

# Delivery cases are about ingestion, not pairs: the test that covers each one.
INGESTION_SCENARIOS = {
    "SOURCE_ID_REUSE": "test_insert_after_delete_starts_a_new_record_life",
    "DUPLICATE_DELIVERY": "test_duplicate_delivery_in_a_later_load_is_a_no_op",
    "OUT_OF_ORDER": "test_out_of_order_event_arriving_a_load_late_is_ignored",
    "LATE_AFTER_DELETE": "test_late_update_after_delete_is_ignored",
    "COLUMN_SHIFT": "test_column_shift_is_rejected",
    "ENCODING_CORRUPTION": "test_mojibake_is_rejected",
    "BULK_REPLAY": "test_replaying_any_earlier_load_does_not_change_final_state",
}


def test_every_catalog_code_has_exactly_one_scenario():
    catalog = set(implementations())
    assert not COVERED & INGESTION_SCENARIOS.keys()
    assert COVERED | INGESTION_SCENARIOS.keys() == catalog
    for test_name in INGESTION_SCENARIOS.values():
        assert callable(getattr(test_ingestion, test_name))


def test_scenario_base_record_is_valid():
    r = record()
    assert (r.first_name, r.last_name, r.dob, r.ssn, r.gender) == (
        "CATHERINE",
        "MILLER",
        date(1980, 5, 6),
        "234567801",
        "F",
    )
