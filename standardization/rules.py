"""Match tiers and blockers for a candidate pair (DESIGN.md sections 8 and 9).

Principle: one imperfect field per auto-match; uncertain cases are no-match.

=====  =======  ======================  ===========  =================
Tier   SSN      DOB                     Names        Reason code
=====  =======  ======================  ===========  =================
1      EXACT    EXACT                   one STRONG   TIER_1_EXACT_IDS
2      NEAR     EXACT, not Jan 1        both STRONG  TIER_2_SSN_NEAR
3      EXACT    NEAR                    both STRONG  TIER_3_DOB_NEAR
4      MISSING  EXACT, not Jan 1        both STRONG  TIER_4_NO_SSN
=====  =======  ======================  ===========  =================

A January 1 DOB is weak (DOB_JAN_FIRST): it still counts as EXACT where the
SSN is also exact, but not in Tiers 2 and 4, where the DOB is the main
evidence (D-032). Tier 4 is a match only when the profile allows it; it keeps
its reason code either way (D-033). Anything else is NO_MATCH_INSUFFICIENT.

Blockers override any tier and create a cannot-link constraint for
clustering. The twins blocker needs both first names present (D-035) and
SSNs that are near or different, not exact (D-036). Every blocker that
applies is recorded; the reason code is the first in this order:
generational, suffix, twins, SSN conflict, newborn.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from pipeline.config import MatchingConfig, Profile
from standardization.compare import (
    DIFFERENT,
    EXACT,
    MISSING,
    NEAR,
    NONE,
    STRONG,
    Comparison,
    NameRules,
    compare_records,
    years_between,
)
from standardization.standardize import StandardRecord

NO_MATCH = "NO_MATCH_INSUFFICIENT"
BLOCKER_ORDER = (
    "BLOCKED_GENERATIONAL",
    "BLOCKED_SUFFIX",
    "BLOCKED_TWINS",
    "BLOCKED_SSN_CONFLICT",
)


@dataclass(frozen=True)
class PairRules:
    """Name rules plus the profile and blocker parameters for one run."""

    names: NameRules
    profile: Profile
    generational_gap_years: int
    newborn_max_age_years: int

    @classmethod
    def build(cls, ref, matching: MatchingConfig, profile_name: str | None = None) -> PairRules:
        profile = matching.profile(profile_name)
        return cls(
            names=NameRules.build(ref, matching, profile),
            profile=profile,
            generational_gap_years=matching.generational_gap_years,
            newborn_max_age_years=matching.newborn_max_age_years,
        )


@dataclass(frozen=True)
class PairDecision:
    comparison: Comparison
    tier: int | None
    reason_code: str
    is_match: bool
    blockers: tuple[str, ...]

    @property
    def cannot_link(self) -> bool:
        return bool(self.blockers)


def match_tier(c: Comparison, dob_is_jan1: bool, profile: Profile) -> tuple[int | None, str, bool]:
    """Return ``(tier, reason_code, is_match)`` before blockers."""
    one_strong = STRONG in (c.first_level, c.last_level)
    both_strong = c.first_level == STRONG and c.last_level == STRONG
    firm_dob = c.dob_level == EXACT and not dob_is_jan1
    if c.ssn_level == EXACT and c.dob_level == EXACT and one_strong:
        return 1, "TIER_1_EXACT_IDS", True
    if c.ssn_level == NEAR and firm_dob and both_strong:
        return 2, "TIER_2_SSN_NEAR", True
    if c.ssn_level == EXACT and c.dob_level == NEAR and both_strong:
        return 3, "TIER_3_DOB_NEAR", True
    if c.ssn_level == MISSING and firm_dob and both_strong:
        return 4, "TIER_4_NO_SSN", profile.tier_4_matches
    return None, NO_MATCH, False


def _is_newborn_placeholder(record: StandardRecord, today: date, max_age_years: int) -> bool:
    return (
        record.has_placeholder_name
        and record.dob is not None
        and record.dob <= today
        and years_between(record.dob, today) < max_age_years
    )


def blockers_for(
    a: StandardRecord, b: StandardRecord, c: Comparison, rules: PairRules, today: date
) -> tuple[str, ...]:
    """Every blocker that applies to the pair, in BLOCKER_ORDER, without duplicates."""
    found = set()
    gap = c.dob_gap_years
    if c.ssn_level in (EXACT, NEAR) and gap is not None and gap >= rules.generational_gap_years:
        found.add("BLOCKED_GENERATIONAL")
    if a.suffix is not None and b.suffix is not None and a.suffix != b.suffix:
        found.add("BLOCKED_SUFFIX")
    if (
        c.dob_level == EXACT
        and c.last_level == STRONG
        and c.first_level == NONE
        and a.first_name is not None  # a missing first name is not a different one (D-035)
        and b.first_name is not None
        and c.ssn_level in (NEAR, DIFFERENT)  # not EXACT: that is a first-name change (D-036)
    ):
        found.add("BLOCKED_TWINS")
    if (
        c.ssn_level == EXACT
        and c.dob_level == DIFFERENT
        and c.first_level == NONE
        and c.last_level == NONE
    ):
        found.add("BLOCKED_SSN_CONFLICT")
    for newborn, other in ((a, b), (b, a)):
        if (
            _is_newborn_placeholder(newborn, today, rules.newborn_max_age_years)
            and other.dob is not None
            and other.dob < newborn.dob
            and years_between(other.dob, newborn.dob) >= rules.generational_gap_years
        ):
            found.add("BLOCKED_GENERATIONAL")
    return tuple(code for code in BLOCKER_ORDER if code in found)


def decide(a: StandardRecord, b: StandardRecord, rules: PairRules, today: date) -> PairDecision:
    """Compare a candidate pair, apply tiers, then blockers (which override tiers)."""
    c = compare_records(a, b, rules.names)
    tier, reason, is_match = match_tier(c, a.dob_is_jan1 and b.dob_is_jan1, rules.profile)
    blockers = blockers_for(a, b, c, rules, today)
    if blockers:
        return PairDecision(c, tier, blockers[0], False, blockers)
    return PairDecision(c, tier, reason, is_match, ())
