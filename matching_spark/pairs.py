"""Blocking, field comparison, tiers and blockers as DataFrame operations.

Mirrors the dbt models ``int_block_keys``, ``int_skipped_blocks``,
``int_candidate_pairs``, ``int_name_form_pairs``, ``int_form_scores``,
``int_name_levels`` and ``match_pairs`` (DESIGN.md sections 6-9), and the
Python reference ``standardization/blocking.py``, ``compare.py`` and ``rules.py``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pyarrow as pa
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from matching_spark.standardize import iso_date
from matching_spark.udfs import double_metaphone_udf, for_python, jaro_winkler_udf
from pipeline.reference import ReferenceData

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
COMBOS = (
    ("first_first", "first", "first"),
    ("last_last", "last", "last"),
    ("first_last", "first", "last"),
    ("last_first", "last", "first"),
)


@dataclass(frozen=True)
class PairParameters:
    """Everything the pair rules read from config/matching.yaml for one profile."""

    profile: str
    strong_name_threshold: float
    weak_name_threshold: float
    tier_4_matches: bool
    prefix_min_length: int
    generational_gap_years: int
    newborn_max_age_years: int
    max_block_size: int
    blocking_keys: tuple[str, ...]


# --------------------------------------------------------------------------- #
# Blocking (section 6, D-040)
# --------------------------------------------------------------------------- #


def year_text(col: Column) -> Column:
    return F.format_string("%04d", F.year(col))


def phonetic_last_name(col: Column) -> Column:
    """The last name as the phonetic blocking key encodes it: no spaces, hyphens, apostrophes."""
    return F.regexp_replace(col, "[ '-]", "")


def name_codes(records: DataFrame) -> DataFrame:
    """(name, codes): Double Metaphone codes of every name string this load needs.

    That is every comparison form, plus every last name as the phonetic
    blocking key encodes it. Computed once per load, in one Python step.
    """
    names = (
        records.select(F.explode(F.concat("first_name_forms", "last_name_forms")).alias("name"))
        .union(
            records.where(F.col("last_name").isNotNull()).select(
                phonetic_last_name(F.col("last_name")).alias("name")
            )
        )
        .distinct()
    )
    return for_python(names).withColumn("codes", double_metaphone_udf("name")).localCheckpoint()


def block_keys(records: DataFrame, codes: DataFrame, keys: tuple[str, ...]) -> DataFrame:
    """(record_key, key_name, key_value) for every blocking key a record has."""
    r = records
    has_ssn, has_dob = F.col("ssn").isNotNull(), F.col("dob").isNotNull()
    last_codes = codes.select(F.col("name").alias("last_key"), "codes")
    parts = [
        r.where(has_ssn).select(
            "record_key", F.lit("ssn").alias("key_name"), F.col("ssn").alias("key_value")
        ),
        r.where(has_dob).select("record_key", F.lit("dob"), iso_date(F.col("dob"))),
        r.where(has_ssn & has_dob).select(
            "record_key",
            F.lit("ssn_sorted_digits_birth_year"),
            F.concat(
                F.array_join(F.array_sort(F.split("ssn", "")), ""),
                F.lit("|"),
                year_text(F.col("dob")),
            ),
        ),
        r.where(has_ssn & has_dob).select(
            "record_key",
            F.lit("ssn_first5_birth_year"),
            F.concat(F.substring("ssn", 1, 5), F.lit("|"), year_text(F.col("dob"))),
        ),
        r.where(F.col("last_name").isNotNull() & F.col("first_name").isNotNull() & has_dob)
        .withColumn("last_key", phonetic_last_name(F.col("last_name")))
        .join(last_codes, "last_key")
        .where(F.size("codes") > 0)
        .select(
            "record_key",
            F.lit("last_phonetic_first_initial_birth_year"),
            F.concat_ws(
                "|",
                F.try_element_at("codes", F.lit(1)),
                F.substring("first_name", 1, 1),
                year_text(F.col("dob")),
            ),
        ),
    ]
    union = parts[0]
    for part in parts[1:]:
        union = union.unionAll(part)
    return union.where(F.col("key_name").isin(*keys))


def skipped_blocks(keys: DataFrame, max_block_size: int) -> DataFrame:
    return (
        keys.groupBy("key_name", "key_value")
        .agg(F.count("*").alias("block_size"))
        .where(F.col("block_size") > max_block_size)
    )


def candidate_pairs(keys: DataFrame, skipped: DataFrame) -> DataFrame:
    """Pairs sharing any key, deduplicated across keys, smaller record key first."""
    kept = keys.join(skipped, ["key_name", "key_value"], "left_anti")
    a, b = kept.alias("a"), kept.alias("b")
    return (
        a.join(
            b,
            (F.col("a.key_name") == F.col("b.key_name"))
            & (F.col("a.key_value") == F.col("b.key_value"))
            & (F.col("a.record_key") < F.col("b.record_key")),
        )
        .groupBy(
            F.col("a.record_key").alias("record_key_a"), F.col("b.record_key").alias("record_key_b")
        )
        .agg(F.array_sort(F.collect_set("a.key_name")).alias("blocking_keys"))
    )


# --------------------------------------------------------------------------- #
# Names (section 7, D-030, D-031)
# --------------------------------------------------------------------------- #


def _with_records(pairs: DataFrame, records: DataFrame, columns: list[str]) -> DataFrame:
    """Pairs with the chosen record columns suffixed _a and _b."""
    a = records.select(
        F.col("record_key").alias("record_key_a"), *[F.col(c).alias(f"{c}_a") for c in columns]
    )
    b = records.select(
        F.col("record_key").alias("record_key_b"), *[F.col(c).alias(f"{c}_b") for c in columns]
    )
    return pairs.join(a, "record_key_a").join(b, "record_key_b")


def name_form_pairs(pairs: DataFrame, records: DataFrame) -> DataFrame:
    p = _with_records(pairs, records, ["first_name_forms", "last_name_forms"])
    parts = [
        p.select(
            "record_key_a",
            "record_key_b",
            F.lit(combo).alias("combo"),
            F.explode(f"{fa}_name_forms_a").alias("form_a"),
            F.col(f"{fb}_name_forms_b").alias("forms_b"),
        ).select(
            "record_key_a", "record_key_b", "combo", "form_a", F.explode("forms_b").alias("form_b")
        )
        for combo, fa, fb in COMBOS
    ]
    union = parts[0]
    for part in parts[1:]:
        union = union.unionAll(part)
    return union


def name_keys_and_ambiguity(spark: SparkSession, ref: ReferenceData) -> tuple[DataFrame, DataFrame]:
    """Name -> sorted canonical names of its groups; names that are variants of both genders."""
    # From Arrow, not a Python list: a list becomes pickled rows that only Python
    # workers can read, and starting those is slow on Windows (see udfs.py).
    variants = spark.createDataFrame(
        pa.table(
            {
                "canonical": [v.canonical for v in ref.name_variants],
                "canonical_gender": [v.canonical_gender for v in ref.name_variants],
                "variant": [v.variant for v in ref.name_variants],
            }
        )
    )
    keys = (
        variants.select(F.col("canonical").alias("name"), "canonical")
        .union(variants.select(F.col("variant").alias("name"), "canonical"))
        .groupBy("name")
        .agg(F.array_sort(F.collect_set("canonical")).alias("keys"))
    )
    ambiguous = (
        variants.groupBy(F.col("variant").alias("name"))
        .agg(
            F.bool_or(F.col("canonical_gender") == "M").alias("m"),
            F.bool_or(F.col("canonical_gender") == "F").alias("f"),
        )
        .where("m and f")
        .select("name")
    )
    return keys, ambiguous


def form_scores(
    form_pairs: DataFrame, codes: DataFrame, name_keys: DataFrame, ambiguous: DataFrame
) -> DataFrame:
    """Pair-independent facts about each distinct pair of forms (compare.py:compare_forms)."""
    distinct = (
        for_python(form_pairs.select("form_a", "form_b").distinct())
        .withColumn("jaro_winkler", jaro_winkler_udf("form_a", "form_b"))
        .localCheckpoint()
    )

    def keyed(side: str) -> DataFrame:
        return name_keys.select(
            F.col("name").alias(f"form_{side}"), F.col("keys").alias(f"keys_{side}")
        )

    def amb(side: str) -> DataFrame:
        return ambiguous.select(
            F.col("name").alias(f"form_{side}"), F.lit(True).alias(f"amb_{side}")
        )

    def coded(side: str) -> DataFrame:
        return codes.select(
            F.col("name").alias(f"form_{side}"), F.col("codes").alias(f"codes_{side}")
        )

    s = distinct
    for side in ("a", "b"):
        s = (
            s.join(F.broadcast(keyed(side)), f"form_{side}", "left")
            .join(F.broadcast(amb(side)), f"form_{side}", "left")
            .join(coded(side), f"form_{side}")
        )
    la, lb = F.length("form_a"), F.length("form_b")
    empty = F.array().cast("array<string>")
    return s.select(
        "form_a",
        "form_b",
        ((la == 1) | (lb == 1)).alias("has_initial"),
        F.when(la <= lb, F.col("form_b").startswith(F.col("form_a")))
        .otherwise(F.col("form_a").startswith(F.col("form_b")))
        .alias("is_prefix"),
        F.least(la, lb).alias("shorter_length"),
        (F.col("form_a") == F.col("form_b")).alias("is_exact"),
        F.arrays_overlap(
            F.array_append(F.coalesce("keys_a", empty), F.col("form_a")),
            F.array_append(F.coalesce("keys_b", empty), F.col("form_b")),
        ).alias("is_related"),
        (F.coalesce("amb_a", F.lit(False)) | F.coalesce("amb_b", F.lit(False))).alias(
            "is_gender_ambiguous"
        ),
        F.arrays_overlap("codes_a", "codes_b").alias("is_phonetic"),
        "jaro_winkler",
    )


def name_levels(
    pairs: DataFrame,
    records: DataFrame,
    form_pairs: DataFrame,
    scores: DataFrame,
    params: PairParameters,
) -> DataFrame:
    """Best (level, method) per pair and field pairing (compare.py:compare_name)."""
    genders = _with_records(pairs, records, ["gender"]).select(
        "record_key_a",
        "record_key_b",
        (
            F.col("gender_a").isNotNull()
            & F.col("gender_b").isNotNull()
            & (F.col("gender_a") != F.col("gender_b"))
        ).alias("genders_conflict"),
    )
    s = form_pairs.join(scores, ["form_a", "form_b"]).join(
        genders, ["record_key_a", "record_key_b"]
    )
    result = (
        F.when(
            F.col("has_initial"), F.when(F.col("is_prefix"), "WEAK|INITIAL").otherwise("NONE|NONE")
        )
        .when(F.col("is_exact"), "STRONG|EXACT")
        .when(
            F.col("is_related") & ~(F.col("is_gender_ambiguous") & F.col("genders_conflict")),
            "STRONG|VARIANT",
        )
        .when(F.col("is_phonetic"), "STRONG|PHONETIC")
        .when(F.col("jaro_winkler") >= params.strong_name_threshold, "STRONG|FUZZY")
        .when(
            (F.col("shorter_length") >= params.prefix_min_length) & F.col("is_prefix"),
            "STRONG|PREFIX",
        )
        .when(F.col("is_related"), "WEAK|VARIANT_GENDER_CONFLICT")
        .when(F.col("jaro_winkler") >= params.weak_name_threshold, "WEAK|FUZZY_WEAK")
        .otherwise("NONE|NONE")
    )
    level = F.split_part(F.col("result"), F.lit("|"), F.lit(1))
    method = F.split_part(F.col("result"), F.lit("|"), F.lit(2))
    rank = F.when(level == "STRONG", 2).when(level == "WEAK", 1).otherwise(0)
    position = F.array_position(F.array(*[F.lit(m) for m in METHODS]), method)
    best = (
        s.withColumn("result", result)
        .withColumn("score", rank * 100 - position)
        .groupBy("record_key_a", "record_key_b", "combo")
        .agg(F.max_by("result", "score").alias("best"))
    )
    return best.select(
        "record_key_a",
        "record_key_b",
        "combo",
        F.split_part("best", F.lit("|"), F.lit(1)).alias("level"),
        F.split_part("best", F.lit("|"), F.lit(2)).alias("method"),
    )


# --------------------------------------------------------------------------- #
# Levels, tiers and blockers (sections 7-9)
# --------------------------------------------------------------------------- #


def years_between(a: Column, b: Column) -> Column:
    """Whole years from the earlier date to the later one, age-style."""
    hi, lo = F.greatest(a, b), F.least(a, b)
    before_birthday = (F.month(hi) * 100 + F.dayofmonth(hi)) < (
        F.month(lo) * 100 + F.dayofmonth(lo)
    )
    return (F.year(hi) - F.year(lo) - F.when(before_birthday, 1).otherwise(0)).cast("bigint")


def hamming(a: str, b: str, length: int) -> Column:
    return F.expr(
        f"aggregate(sequence(1, {length}), 0, (acc, i) -> "
        f"acc + if(substr({a}, i, 1) <> substr({b}, i, 1), 1, 0))"
    )


def _ssn_level() -> Column:
    adjacent_swap = F.expr(
        "exists(sequence(1, 8), i -> substr(ssn_a, i, 1) <> substr(ssn_b, i, 1)"
        " and substr(ssn_a, i, 1) = substr(ssn_b, i + 1, 1)"
        " and substr(ssn_a, i + 1, 1) = substr(ssn_b, i, 1))"
    )
    distance = hamming("ssn_a", "ssn_b", 9)
    return (
        F.when(F.col("ssn_a").isNull() | F.col("ssn_b").isNull(), "MISSING")
        .when(F.col("ssn_a") == F.col("ssn_b"), "EXACT")
        .when(distance == 1, "NEAR")
        .when((distance == 2) & adjacent_swap, "NEAR")
        .otherwise("DIFFERENT")
    )


def _dob_level() -> Column:
    a, b = F.col("dob_a"), F.col("dob_b")
    ya, ma, da = F.year(a), F.month(a), F.dayofmonth(a)
    yb, mb, db = F.year(b), F.month(b), F.dayofmonth(b)

    def leap_shift(m1, d1, m2, d2, y1, y2):
        return (
            (m1 == 2)
            & (d1 == 29)
            & (y2 == y1)
            & (((m2 == 2) & (d2 == 28)) | ((m2 == 3) & (d2 == 1)))
        )

    near = (
        ((ya == yb) & (ma == db) & (da == mb))
        | (hamming("dob_digits_a", "dob_digits_b", 8) == 1)
        | ((F.abs(ya - yb) == 1) & (ma == mb) & (da == db))
        | leap_shift(ma, da, mb, db, ya, yb)
        | leap_shift(mb, db, ma, da, yb, ya)
    )
    return (
        F.when(a.isNull() | b.isNull(), "MISSING")
        .when(a == b, "EXACT")
        .when(near, "NEAR")
        .otherwise("DIFFERENT")
    )


def match_pairs(
    pairs: DataFrame,
    records: DataFrame,
    levels: DataFrame,
    params: PairParameters,
    load_id: int,
    today: date,
) -> DataFrame:
    """Every compared pair with levels, tier, blockers, reason code and decision."""
    p = _with_records(
        pairs,
        records,
        [
            "ssn",
            "dob",
            "dob_is_jan1",
            "suffix",
            "first_name",
            "first_name_is_placeholder",
            "last_name_is_placeholder",
        ],
    )
    names = levels.groupBy("record_key_a", "record_key_b").agg(
        *[
            F.max(F.when(F.col("combo") == combo, F.col(column))).alias(f"{short}_{column}")
            for combo, short in (
                ("first_first", "ff"),
                ("last_last", "ll"),
                ("first_last", "fl"),
                ("last_first", "lf"),
            )
            for column in ("level", "method")
        ]
    )
    digits = "%04d%02d%02d"
    df = (
        p.withColumn(
            "dob_digits_a",
            F.format_string(digits, F.year("dob_a"), F.month("dob_a"), F.dayofmonth("dob_a")),
        )
        .withColumn(
            "dob_digits_b",
            F.format_string(digits, F.year("dob_b"), F.month("dob_b"), F.dayofmonth("dob_b")),
        )
        .join(names, ["record_key_a", "record_key_b"], "left")
        .withColumn("ssn_level", _ssn_level())
        .withColumn("dob_level", _dob_level())
        .withColumn(
            "dob_gap_years",
            F.when(
                F.col("dob_a").isNotNull() & F.col("dob_b").isNotNull(),
                years_between(F.col("dob_a"), F.col("dob_b")),
            ),
        )
    )
    for short in ("ff", "ll", "fl", "lf"):
        df = df.withColumn(f"{short}_level", F.coalesce(f"{short}_level", F.lit("NONE")))
        df = df.withColumn(f"{short}_method", F.coalesce(f"{short}_method", F.lit("MISSING")))

    strong = F.lit("STRONG")
    swapped = (
        ~((F.col("ff_level") == strong) & (F.col("ll_level") == strong))
        & (F.col("fl_level") == strong)
        & (F.col("lf_level") == strong)
    )
    df = df.withColumn("names_swapped", swapped)
    for field, direct, cross in (("first", "ff", "fl"), ("last", "ll", "lf")):
        for column in ("level", "method"):
            df = df.withColumn(
                f"{field}_{column}",
                F.when(F.col("names_swapped"), F.col(f"{cross}_{column}")).otherwise(
                    F.col(f"{direct}_{column}")
                ),
            )

    ssn, dob = F.col("ssn_level"), F.col("dob_level")
    first, last = F.col("first_level"), F.col("last_level")
    jan1 = F.col("dob_is_jan1_a") & F.col("dob_is_jan1_b")
    both_strong = (first == strong) & (last == strong)
    tier = (
        F.when((ssn == "EXACT") & (dob == "EXACT") & ((first == strong) | (last == strong)), 1)
        .when((ssn == "NEAR") & (dob == "EXACT") & ~jan1 & both_strong, 2)
        .when((ssn == "EXACT") & (dob == "NEAR") & both_strong, 3)
        .when((ssn == "MISSING") & (dob == "EXACT") & ~jan1 & both_strong, 4)
    )
    df = df.withColumn("tier", tier.cast("int"))

    gap = params.generational_gap_years
    today_col = F.lit(today)

    def newborn_against(x: str, y: str) -> Column:
        placeholder = F.col(f"first_name_is_placeholder_{x}") | F.col(
            f"last_name_is_placeholder_{x}"
        )
        dob_x, dob_y = F.col(f"dob_{x}"), F.col(f"dob_{y}")
        return (
            placeholder
            & (dob_x <= today_col)
            & (years_between(dob_x, today_col) < params.newborn_max_age_years)
            & (dob_y < dob_x)
            & (years_between(dob_y, dob_x) >= gap)
        )

    blockers = F.array(
        F.when(
            (ssn.isin("EXACT", "NEAR") & (F.col("dob_gap_years") >= gap))
            | newborn_against("a", "b")
            | newborn_against("b", "a"),
            "BLOCKED_GENERATIONAL",
        ),
        F.when(
            F.col("suffix_a").isNotNull()
            & F.col("suffix_b").isNotNull()
            & (F.col("suffix_a") != F.col("suffix_b")),
            "BLOCKED_SUFFIX",
        ),
        F.when(
            (dob == "EXACT")
            & (last == strong)
            & (first == "NONE")
            & F.col("first_name_a").isNotNull()
            & F.col("first_name_b").isNotNull()
            & ssn.isin("NEAR", "DIFFERENT"),
            "BLOCKED_TWINS",
        ),
        F.when(
            (ssn == "EXACT") & (dob == "DIFFERENT") & (first == "NONE") & (last == "NONE"),
            "BLOCKED_SSN_CONFLICT",
        ),
    )
    df = df.withColumn("blockers", F.filter(blockers, lambda code: code.isNotNull()))
    blocked = F.size("blockers") > 0
    tier_col = F.col("tier")
    reason = (
        F.when(blocked, F.try_element_at("blockers", F.lit(1)))
        .when(tier_col == 1, "TIER_1_EXACT_IDS")
        .when(tier_col == 2, "TIER_2_SSN_NEAR")
        .when(tier_col == 3, "TIER_3_DOB_NEAR")
        .when(tier_col == 4, "TIER_4_NO_SSN")
        .otherwise("NO_MATCH_INSUFFICIENT")
    )
    matches = tier_col.isin(1, 2, 3) | ((tier_col == 4) & F.lit(params.tier_4_matches))
    return df.select(
        F.lit(load_id).cast("int").alias("load_id"),
        F.lit(params.profile).alias("profile"),
        "record_key_a",
        "record_key_b",
        "blocking_keys",
        "ssn_level",
        "dob_level",
        "dob_gap_years",
        "first_level",
        "first_method",
        "last_level",
        "last_method",
        "names_swapped",
        "tier",
        "blockers",
        reason.alias("reason_code"),
        (~blocked & F.coalesce(matches, F.lit(False))).alias("is_match"),
        blocked.alias("cannot_link"),
    )
