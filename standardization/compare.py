"""Field comparison levels for a pair of standardized records (DESIGN.md section 7).

SSN: EXACT, NEAR (one substitution or one adjacent transposition), MISSING,
DIFFERENT.

DOB: EXACT, NEAR (month/day swap, one-digit typo, year +/-1 with the same
month and day, Feb 29 recorded as Feb 28 or Mar 1), MISSING, DIFFERENT. The
gap in whole years is computed too.

Names: STRONG, WEAK or NONE, the best result over every pair of comparison
forms (D-030). For two forms ``a`` and ``b``:

* A single letter against a name starting with it is WEAK (an initial), and
  never stronger, even against the same letter.
* STRONG: equal; related through the name variant table (D-031); sharing a
  Double Metaphone code; Jaro-Winkler at or above the strong threshold; or one
  a prefix of the other with the shorter at least ``prefix_min_length`` long.
* WEAK: Jaro-Winkler at or above the weak threshold.
* A variant-table match through a gender-ambiguous name (CHRIS, PAT, ...)
  between records with different non-null genders counts as WEAK only.

If first and last are not both STRONG but the cross comparisons (first
against last and last against first) are, both are STRONG and the pair is
flagged ``names_swapped``. Gender is never positive evidence.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date

from pipeline.config import MatchingConfig, Profile
from pipeline.reference import ReferenceData
from standardization.similarity import jaro_winkler, phonetic_match
from standardization.standardize import StandardRecord

EXACT, NEAR, MISSING, DIFFERENT = "EXACT", "NEAR", "MISSING", "DIFFERENT"
STRONG, WEAK, NONE = "STRONG", "WEAK", "NONE"
_RANK = {STRONG: 2, WEAK: 1, NONE: 0}

# Name methods, most to least specific; ties between equal levels keep the first.
METHODS = (
    "EXACT",
    "VARIANT",
    "PHONETIC",
    "FUZZY",
    "PREFIX",
    "VARIANT_GENDER_CONFLICT",
    "FUZZY_WEAK",
    "INITIAL",
    "NONE",
    "MISSING",
)


# --------------------------------------------------------------------------- #
# SSN and DOB
# --------------------------------------------------------------------------- #


def _hamming_one(a: str, b: str) -> bool:
    return len(a) == len(b) and sum(x != y for x, y in zip(a, b, strict=True)) == 1


def _adjacent_transposition(a: str, b: str) -> bool:
    if len(a) != len(b):
        return False
    diffs = [i for i, (x, y) in enumerate(zip(a, b, strict=True)) if x != y]
    return (
        len(diffs) == 2
        and diffs[1] == diffs[0] + 1
        and a[diffs[0]] == b[diffs[1]]
        and a[diffs[1]] == b[diffs[0]]
    )


def compare_ssn(a: str | None, b: str | None) -> str:
    if a is None or b is None:
        return MISSING
    if a == b:
        return EXACT
    if _hamming_one(a, b) or _adjacent_transposition(a, b):
        return NEAR
    return DIFFERENT


def _is_leap_shift(a: date, b: date) -> bool:
    for leap, other in ((a, b), (b, a)):
        if (
            (leap.month, leap.day) == (2, 29)
            and other.year == leap.year
            and (other.month, other.day) in ((2, 28), (3, 1))
        ):
            return True
    return False


def dob_is_near(a: date, b: date) -> bool:
    """One of the four DOB near-miss patterns (for distinct dates)."""
    if a.year == b.year and (a.month, a.day) == (b.day, b.month):
        return True
    if _hamming_one(a.strftime("%Y%m%d"), b.strftime("%Y%m%d")):
        return True
    if abs(a.year - b.year) == 1 and (a.month, a.day) == (b.month, b.day):
        return True
    return _is_leap_shift(a, b)


def compare_dob(a: date | None, b: date | None) -> str:
    if a is None or b is None:
        return MISSING
    if a == b:
        return EXACT
    return NEAR if dob_is_near(a, b) else DIFFERENT


def years_between(a: date | None, b: date | None) -> int | None:
    """Whole years from the earlier to the later date (age-style); None if either is missing."""
    if a is None or b is None:
        return None
    older, younger = sorted((a, b))
    return younger.year - older.year - ((younger.month, younger.day) < (older.month, older.day))


# --------------------------------------------------------------------------- #
# Names
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class NameRules:
    """Everything name comparison needs: variant groups, thresholds, prefix rule."""

    groups: dict[str, frozenset[str]]
    gender_ambiguous: frozenset[str]
    strong_threshold: float
    weak_threshold: float
    prefix_min_length: int

    @classmethod
    def build(cls, ref: ReferenceData, matching: MatchingConfig, profile: Profile) -> NameRules:
        groups: dict[str, set[str]] = defaultdict(set)
        for row in ref.name_variants:
            groups[row.canonical].add(row.canonical)
            groups[row.variant].add(row.canonical)
        return cls(
            groups={name: frozenset(g) for name, g in groups.items()},
            gender_ambiguous=ref.gender_ambiguous_variants(),
            strong_threshold=profile.strong_name_threshold,
            weak_threshold=profile.weak_name_threshold,
            prefix_min_length=matching.name_prefix_min_length,
        )

    def related(self, a: str, b: str) -> bool:
        """Same name group: one is a variant of the other, or both of one canonical (D-031)."""
        return bool(
            (self.groups.get(a, frozenset()) | {a}) & (self.groups.get(b, frozenset()) | {b})
        )


def compare_forms(a: str, b: str, rules: NameRules, genders_conflict: bool) -> tuple[str, str]:
    """Level and method for one pair of comparison forms."""
    if len(a) == 1 or len(b) == 1:
        short, long = sorted((a, b), key=len)
        if long.startswith(short):
            return WEAK, "INITIAL"
        return NONE, "NONE"
    if a == b:
        return STRONG, "EXACT"
    if rules.related(a, b):
        ambiguous = a in rules.gender_ambiguous or b in rules.gender_ambiguous
        if not (ambiguous and genders_conflict):
            return STRONG, "VARIANT"
        best = (WEAK, "VARIANT_GENDER_CONFLICT")
    else:
        best = (NONE, "NONE")
    if phonetic_match(a, b):
        return STRONG, "PHONETIC"
    score = jaro_winkler(a, b)
    if score >= rules.strong_threshold:
        return STRONG, "FUZZY"
    short, long = sorted((a, b), key=len)
    if len(short) >= rules.prefix_min_length and long.startswith(short):
        return STRONG, "PREFIX"
    if best[0] == WEAK:
        return best
    if score >= rules.weak_threshold:
        return WEAK, "FUZZY_WEAK"
    return NONE, "NONE"


def compare_name(
    forms_a: tuple[str, ...], forms_b: tuple[str, ...], rules: NameRules, genders_conflict: bool
) -> tuple[str, str]:
    """Best level (and its method) over all pairs of forms; NONE/MISSING if either is empty."""
    if not forms_a or not forms_b:
        return NONE, "MISSING"
    best = (NONE, "NONE")
    for a in forms_a:
        for b in forms_b:
            result = compare_forms(a, b, rules, genders_conflict)
            if (_RANK[result[0]], -METHODS.index(result[1])) > (
                _RANK[best[0]],
                -METHODS.index(best[1]),
            ):
                best = result
    return best


# --------------------------------------------------------------------------- #
# Whole pair
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Comparison:
    ssn_level: str
    dob_level: str
    dob_gap_years: int | None
    first_level: str
    last_level: str
    first_method: str
    last_method: str
    names_swapped: bool


def compare_records(a: StandardRecord, b: StandardRecord, rules: NameRules) -> Comparison:
    conflict = a.gender is not None and b.gender is not None and a.gender != b.gender
    first = compare_name(a.first_name_forms, b.first_name_forms, rules, conflict)
    last = compare_name(a.last_name_forms, b.last_name_forms, rules, conflict)
    swapped = False
    if not (first[0] == STRONG and last[0] == STRONG):
        cross_1 = compare_name(a.first_name_forms, b.last_name_forms, rules, conflict)
        cross_2 = compare_name(a.last_name_forms, b.first_name_forms, rules, conflict)
        if cross_1[0] == STRONG and cross_2[0] == STRONG:
            first, last, swapped = cross_1, cross_2, True
    return Comparison(
        ssn_level=compare_ssn(a.ssn, b.ssn),
        dob_level=compare_dob(a.dob, b.dob),
        dob_gap_years=years_between(a.dob, b.dob),
        first_level=first[0],
        last_level=last[0],
        first_method=first[1],
        last_method=last[1],
        names_swapped=swapped,
    )
