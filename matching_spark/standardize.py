"""Standardization as DataFrame operations (DESIGN.md section 5).

Mirrors ``matching_dbt/models/intermediate/int_records_standardized.sql`` and
the Python reference ``standardization/standardize.py``; ``tests/test_spark.py``
checks all three agree.

Reference tables are small, so they become literal map and array columns
rather than joins. Spark has no NFC function; the letter table makes NFC
redundant, which ``tests/test_reference.py`` proves for every letter (D-029).
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from pipeline.reference import ReferenceData
from standardization.standardize import StandardizationTables

# chr(8217), chr(8216), chr(700), chr(96), chr(180): typographic apostrophes.
TYPOGRAPHIC_APOSTROPHES = "’‘ʼ`´"
RAW_WHITESPACE = " \t\r\n"

SCIENTIFIC = r"^([0-9])(?:\.([0-9]+))?E\+?([0-9]+)$"
FLOAT_ZERO = r"^([0-9]+)\.0+$"
DOB_ISO = r"^([0-9]{4})-([0-9]{1,2})-([0-9]{1,2})(?:[ T].*)?$"
DOB_US = r"^([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})$"
DOB_US_SHORT = r"^([0-9]{1,2})/([0-9]{1,2})/([0-9]{2})$"
DOB_COMPACT = r"^([0-9]{4})([0-9]{2})([0-9]{2})$"


def literal_map(mapping: dict[str, str]) -> Column:
    """A map column from a Python dict, keys sorted for a stable plan."""
    items = sorted(mapping.items())
    return F.map_from_arrays(
        F.array(*[F.lit(k) for k, _ in items]), F.array(*[F.lit(v) for _, v in items])
    )


def literal_array(values) -> Column:
    return F.array(*[F.lit(v) for v in sorted(values)]).cast("array<string>")


def trim_raw(col: Column) -> Column:
    """Raw SSN/DOB/gender values are trimmed of ASCII whitespace only (D-039)."""
    return F.btrim(col, F.lit(RAW_WHITESPACE))


def nullif_empty(col: Column) -> Column:
    return F.when(col != "", col)


# --------------------------------------------------------------------------- #
# Names
# --------------------------------------------------------------------------- #


def map_letters(col: Column, letters: dict[str, str]) -> Column:
    """Apply the letter table (D-029): one ``translate`` for single-letter results,
    and one ``regexp_replace`` per multi-letter result (AE, SS, ...).

    Not a map column: Spark inlines a literal map into the generated code at
    every use, and two 527-entry maps make the code too large to compile.
    """
    single = {k: v for k, v in sorted(letters.items()) if len(v) == 1}
    text = F.translate(col, "".join(single), "".join(single.values()))
    by_result: dict[str, list[str]] = {}
    for letter, result in sorted(letters.items()):
        if len(result) != 1:
            by_result.setdefault(result, []).append(letter)
    for result, sources in sorted(by_result.items()):
        text = F.regexp_replace(text, "[" + "".join(sources) + "]", result)
    return text


def clean_name(col: Column, letters: dict[str, str]) -> Column:
    """Name steps 1-4: letter table, drop marks, apostrophes, ASCII, uppercase,
    punctuation, hyphens/apostrophes only between letters or digits, spaces."""
    text = map_letters(col, letters)
    text = F.regexp_replace(text, r"[\x{0300}-\x{036F}]", "")
    text = F.translate(text, TYPOGRAPHIC_APOSTROPHES, "'" * len(TYPOGRAPHIC_APOSTROPHES))
    text = F.upper(F.regexp_replace(text, r"[^\x00-\x7F]", " "))
    text = F.regexp_replace(text, r"[^A-Z0-9' -]", " ")
    text = F.regexp_replace(text, r"['-]{2,}", " ")
    text = F.regexp_replace(text, r"(^|[^A-Z0-9])['-]", "$1 ")
    text = F.regexp_replace(text, r"['-]($|[^A-Z0-9])", " $1")
    return nullif_empty(F.trim(F.regexp_replace(text, " +", " ")))


def plain_name(col: Column) -> Column:
    """Without apostrophes, with hyphens as spaces."""
    text = F.regexp_replace(F.regexp_replace(col, "'", ""), "-", " ")
    return F.trim(F.regexp_replace(text, " +", " "))


def last_token(col: Column) -> Column:
    return nullif_empty(F.regexp_extract(col, " ([^ ]+)$", 1))


def strip_particle(col: Column, particles: tuple[str, ...]) -> Column:
    """The name without its leading particle (longest first), or null."""
    result = F.lit(None).cast("string")
    for particle in reversed(particles):  # build so the first particle is tested first
        if particle.endswith("'"):
            matches = col.startswith(particle) & (F.length(col) > len(particle))
            stripped = F.substring(col, len(particle) + 1, 1_000_000)
        else:
            matches = col.startswith(particle + " ")
            stripped = F.substring(col, len(particle) + 2, 1_000_000)
        result = F.when(matches, stripped).otherwise(result)
    return result


def name_forms(col: Column, particles: tuple[str, ...], particle_words: Column) -> Column:
    """All comparison forms of a cleaned name, sorted (D-030); empty when null."""
    stripped = strip_particle(col, particles)
    plain = plain_name(col)
    plain_stripped = plain_name(stripped)
    tokens = F.filter(
        F.split(plain, " "),
        lambda tok: (F.length(tok) > 1) & ~F.array_contains(particle_words, tok),
    )
    candidates = F.concat(
        F.array(
            col,
            plain,
            F.regexp_replace(plain, " ", ""),
            stripped,
            plain_stripped,
            F.regexp_replace(plain_stripped, " ", ""),
        ),
        F.when(F.size(tokens) > 1, tokens).otherwise(F.array().cast("array<string>")),
    )
    forms = F.array_sort(
        F.array_distinct(F.filter(candidates, lambda f: f.isNotNull() & (f != "")))
    )
    return F.when(col.isNull(), F.array().cast("array<string>")).otherwise(forms)


# --------------------------------------------------------------------------- #
# SSN (D-038)
# --------------------------------------------------------------------------- #


def _with_ssn(df: DataFrame, placeholders: Column) -> DataFrame:
    """Add ``ssn`` and ``ssn_null_reason`` from ``ssn_raw``, in materialized stages.

    Written as one expression, Spark inlines every intermediate value into every
    use, and the generated code grows past what Spark can compile; it then falls
    back to interpreting the expression row by row, many times slower.
    """
    s = F.col("_ssn_text")
    df = df.withColumn("_ssn_text", F.upper(trim_raw(F.col("ssn_raw")))).localCheckpoint()
    df = df.select(
        "*",
        s.rlike(SCIENTIFIC).alias("_ssn_scientific"),
        s.rlike(FLOAT_ZERO).alias("_ssn_float_zero"),
        F.regexp_extract(s, SCIENTIFIC, 2).alias("_ssn_mantissa"),
        F.regexp_extract(s, SCIENTIFIC, 3).try_cast("bigint").alias("_ssn_exponent"),
    ).localCheckpoint()
    scientific, float_zero = F.col("_ssn_scientific"), F.col("_ssn_float_zero")
    mantissa, exponent = F.col("_ssn_mantissa"), F.col("_ssn_exponent")
    df = df.select(
        "*",
        (
            scientific
            & (
                (F.length(mantissa) < exponent)
                | (
                    F.regexp_replace(F.substring(mantissa, exponent + 1, 1_000_000), "^0+", "")
                    != ""
                )
            )
        ).alias("_ssn_unrecoverable"),
        F.when(
            scientific,
            F.concat(F.regexp_extract(s, SCIENTIFIC, 1), F.substring(mantissa, 1, exponent)),
        )
        .when(float_zero, F.regexp_extract(s, FLOAT_ZERO, 1))
        .otherwise(F.regexp_replace(s, "[^0-9]", ""))
        .alias("_ssn_digits"),
        (scientific | float_zero | s.rlike("^[0-9]+$")).alias("_ssn_numeric"),
    ).localCheckpoint()
    digits = F.col("_ssn_digits")
    df = df.withColumn("_ssn_padded", F.lpad(digits, 9, "0")).localCheckpoint()
    padded = F.col("_ssn_padded")
    reason = (
        F.when(F.col("ssn_raw").isNull() | (s == ""), "MISSING")
        .when(F.col("_ssn_unrecoverable"), "UNRECOVERABLE")
        .when(digits == "", "UNPARSEABLE")
        .when(
            (F.length(digits) > 9) | ((F.length(digits) < 9) & ~F.col("_ssn_numeric")),
            "INVALID_LENGTH",
        )
        .when(F.array_contains(placeholders, padded), "PLACEHOLDER")
        .when(padded.startswith("9"), "ITIN_OR_TEMPORARY")
        .when(F.substring(padded, 1, 3).isin("000", "666"), "INVALID_AREA")
        .when(F.substring(padded, 4, 2) == "00", "INVALID_GROUP")
        .when(F.substring(padded, 6, 4) == "0000", "INVALID_SERIAL")
    )
    df = df.withColumn("ssn_null_reason", reason).localCheckpoint()
    return df.withColumn("ssn", F.when(F.col("ssn_null_reason").isNull(), padded))


# --------------------------------------------------------------------------- #
# DOB (D-038)
# --------------------------------------------------------------------------- #


def iso_date(col: Column) -> Column:
    """YYYY-MM-DD with a zero-padded year."""
    return F.format_string("%04d-%02d-%02d", F.year(col), F.month(col), F.dayofmonth(col))


def _with_dob(df: DataFrame, placeholders: Column, today: date) -> DataFrame:
    """Add ``dob`` and ``dob_null_reason`` from ``dob_raw``, in materialized stages.

    Staged for the same reason as ``_with_ssn``.
    """
    t = F.col("_dob_text")
    df = df.withColumn("_dob_text", trim_raw(F.col("dob_raw"))).localCheckpoint()
    df = df.withColumn(
        "_dob_format",
        F.when(t.rlike(DOB_ISO), "ISO")
        .when(t.rlike(DOB_US), "US")
        .when(t.rlike(DOB_US_SHORT), "US_SHORT")
        .when(t.rlike(DOB_COMPACT), "COMPACT"),
    ).localCheckpoint()
    fmt = F.col("_dob_format")

    def part(index_by_format: dict[str, int]) -> Column:
        result = F.lit(None).cast("int")
        for name, pattern in (
            ("ISO", DOB_ISO),
            ("US", DOB_US),
            ("US_SHORT", DOB_US_SHORT),
            ("COMPACT", DOB_COMPACT),
        ):
            value = F.regexp_extract(t, pattern, index_by_format[name]).cast("int")
            result = F.when(fmt == name, value).otherwise(result)
        return result

    df = df.select(
        "*",
        (
            part({"ISO": 1, "US": 3, "US_SHORT": 3, "COMPACT": 1})
            + F.when(fmt == "US_SHORT", 2000).otherwise(0)
        ).alias("_dob_y"),
        part({"ISO": 2, "US": 1, "US_SHORT": 1, "COMPACT": 2}).alias("_dob_m"),
        part({"ISO": 3, "US": 2, "US_SHORT": 2, "COMPACT": 3}).alias("_dob_d"),
    ).localCheckpoint()
    y = F.col("_dob_y")
    df = df.withColumn(
        "_dob_parsed",
        F.when(
            y >= 1,
            F.try_to_date(
                F.format_string("%04d-%02d-%02d", y, F.col("_dob_m"), F.col("_dob_d")),
                "yyyy-MM-dd",
            ),
        ),
    ).localCheckpoint()
    parsed = F.col("_dob_parsed")
    reason = (
        F.when(F.col("dob_raw").isNull() | (t == ""), "MISSING")
        .when(t.rlike("^[0-9]{4}$"), "YEAR_ONLY")
        .when(fmt.isNull(), "UNPARSEABLE")
        .when(parsed.isNull(), "INVALID_DATE")
        .when(F.array_contains(placeholders, iso_date(parsed)), "PLACEHOLDER")
        .when(parsed > F.lit(today), "FUTURE")
    )
    df = df.withColumn("dob_null_reason", reason).localCheckpoint()
    return df.withColumn("dob", F.when(F.col("dob_null_reason").isNull(), parsed))


# --------------------------------------------------------------------------- #
# Records
# --------------------------------------------------------------------------- #

STANDARDIZED_COLUMNS = (
    "source_system",
    "source_record_id",
    "record_life",
    "record_key",
    "record_uid",
    "first_name_raw",
    "last_name_raw",
    "dob_raw",
    "ssn_raw",
    "gender_raw",
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


def standardize_records(records: DataFrame, ref: ReferenceData, today: date) -> DataFrame:
    """One row per live record, with the same columns as dbt's records_standardized."""
    tables = StandardizationTables.from_reference(ref)
    suffixes = literal_map(tables.suffixes)
    name_placeholders = literal_array(tables.name_placeholders)
    particle_words = literal_array(tables.particle_words)

    df = records.select(
        "source_system",
        "source_record_id",
        F.col("record_life").cast("bigint").alias("record_life"),
        F.concat_ws("|", "source_system", "source_record_id").alias("record_key"),
        F.concat_ws(
            "|", "source_system", "source_record_id", F.col("record_life").cast("string")
        ).alias("record_uid"),
        F.col("first_name").alias("first_name_raw"),
        F.col("last_name").alias("last_name_raw"),
        F.col("dob").alias("dob_raw"),
        F.col("ssn").alias("ssn_raw"),
        F.col("gender").alias("gender_raw"),
        clean_name(F.col("first_name"), tables.letters).alias("first_clean"),
        clean_name(F.col("last_name"), tables.letters).alias("last_clean"),
    )
    # Spark inlines each derived column into the next, so without materializing
    # here the per-character cleaning would be recomputed for every later use.
    df = df.localCheckpoint()
    # Step 5: a trailing suffix token, if another token remains.
    for field in ("first", "last"):
        clean = F.col(f"{field}_clean")
        suffix = F.try_element_at(suffixes, last_token(clean))
        df = df.withColumn(f"{field}_suffix", suffix).withColumn(
            f"{field}_unsuffixed",
            F.when(suffix.isNull(), clean).otherwise(F.regexp_replace(clean, " [^ ]+$", "")),
        )
    # Step 6: placeholders become null.
    for field in ("first", "last"):
        unsuffixed = F.col(f"{field}_unsuffixed")
        is_placeholder = F.coalesce(F.array_contains(name_placeholders, unsuffixed), F.lit(False))
        df = df.withColumn(f"{field}_name_is_placeholder", is_placeholder).withColumn(
            f"{field}_name", F.when(~is_placeholder, unsuffixed)
        )
    df = df.localCheckpoint()  # the forms read each name many times
    for field in ("first", "last"):
        df = df.withColumn(
            f"{field}_name_forms",
            name_forms(F.col(f"{field}_name"), tables.particles, particle_words),
        )

    df = _with_ssn(df.localCheckpoint(), literal_array(tables.ssn_placeholders))
    df = _with_dob(df, literal_array(tables.dob_placeholders), today)
    df = (
        df.withColumn("suffix", F.coalesce("last_suffix", "first_suffix"))
        .withColumn(
            "dob_is_jan1",
            F.coalesce((F.month("dob") == 1) & (F.dayofmonth("dob") == 1), F.lit(False)),
        )
        .withColumn(
            "gender",
            F.try_element_at(literal_map(tables.genders), F.upper(trim_raw(F.col("gender_raw")))),
        )
    )
    return df.select(*STANDARDIZED_COLUMNS)
