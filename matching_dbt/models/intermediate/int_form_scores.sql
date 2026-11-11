-- Pair-independent facts about each distinct pair of name forms; mirrors
-- standardization/compare.py:compare_forms. Jaro-Winkler and Double Metaphone
-- come from the shared Python UDFs (D-009).

with form_pairs as (
    select distinct form_a, form_b from {{ ref('int_name_form_pairs') }}
),

variants as (
    select * from {{ ref('stg_ref_name_variants') }}
),

-- Names in the same group share a canonical name (D-031).
name_keys as (
    select name, list(distinct canonical order by canonical) as keys
    from (
        select canonical as name, canonical from variants
        union
        select variant as name, canonical from variants
    )
    group by name
),

-- Variants of canonical names of both genders (CHRIS, PAT, ...).
gender_ambiguous as (
    select variant as name
    from variants
    group by variant
    having bool_or(canonical_gender = 'M') and bool_or(canonical_gender = 'F')
),

forms as (
    select form_a as form from form_pairs
    union
    select form_b from form_pairs
),

codes as (
    select form, pmp_double_metaphone(form) as codes from forms
)

select
    fp.form_a,
    fp.form_b,
    length(fp.form_a) = 1 or length(fp.form_b) = 1 as has_initial,
    case when length(fp.form_a) <= length(fp.form_b) then starts_with(fp.form_b, fp.form_a)
         else starts_with(fp.form_a, fp.form_b) end as is_prefix,
    least(length(fp.form_a), length(fp.form_b)) as shorter_length,
    fp.form_a = fp.form_b as is_exact,
    list_has_any(
        list_append(coalesce(ka.keys, []::varchar[]), fp.form_a),
        list_append(coalesce(kb.keys, []::varchar[]), fp.form_b)
    ) as is_related,
    ga.name is not null or gb.name is not null as is_gender_ambiguous,
    list_has_any(ca.codes, cb.codes) as is_phonetic,
    pmp_jaro_winkler(fp.form_a, fp.form_b) as jaro_winkler
from form_pairs fp
left join name_keys ka on ka.name = fp.form_a
left join name_keys kb on kb.name = fp.form_b
left join gender_ambiguous ga on ga.name = fp.form_a
left join gender_ambiguous gb on gb.name = fp.form_b
left join codes ca on ca.form = fp.form_a
left join codes cb on cb.form = fp.form_b
