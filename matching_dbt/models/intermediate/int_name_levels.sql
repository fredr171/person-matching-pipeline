-- Best name level and method per candidate pair and field pairing; mirrors
-- standardization/compare.py:compare_name. Ties keep the method listed first
-- in compare.METHODS.

with pair_genders as (
    select
        p.record_key_a,
        p.record_key_b,
        a.gender is not null and b.gender is not null and a.gender <> b.gender as genders_conflict
    from {{ ref('int_candidate_pairs') }} p
    join {{ ref('int_records_standardized') }} a on a.record_key = p.record_key_a
    join {{ ref('int_records_standardized') }} b on b.record_key = p.record_key_b
),

scored as (
    select
        fp.record_key_a,
        fp.record_key_b,
        fp.combo,
        case
            when s.has_initial then
                case when s.is_prefix then 'WEAK|INITIAL' else 'NONE|NONE' end
            when s.is_exact then 'STRONG|EXACT'
            when s.is_related and not (s.is_gender_ambiguous and g.genders_conflict) then 'STRONG|VARIANT'
            when s.is_phonetic then 'STRONG|PHONETIC'
            when s.jaro_winkler >= {{ var('strong_name_threshold') }} then 'STRONG|FUZZY'
            when s.shorter_length >= {{ var('prefix_min_length') }} and s.is_prefix then 'STRONG|PREFIX'
            when s.is_related then 'WEAK|VARIANT_GENDER_CONFLICT'
            when s.jaro_winkler >= {{ var('weak_name_threshold') }} then 'WEAK|FUZZY_WEAK'
            else 'NONE|NONE'
        end as result
    from {{ ref('int_name_form_pairs') }} fp
    join {{ ref('int_form_scores') }} s using (form_a, form_b)
    join pair_genders g using (record_key_a, record_key_b)
),

ranked as (
    select
        *,
        case split_part(result, '|', 1) when 'STRONG' then 2 when 'WEAK' then 1 else 0 end * 100
        - list_position(
            ['EXACT', 'VARIANT', 'PHONETIC', 'FUZZY', 'PREFIX', 'VARIANT_GENDER_CONFLICT',
             'FUZZY_WEAK', 'INITIAL', 'NONE', 'MISSING'],
            split_part(result, '|', 2)
          ) as score
    from scored
)

select
    record_key_a,
    record_key_b,
    combo,
    split_part(arg_max(result, score), '|', 1) as level,
    split_part(arg_max(result, score), '|', 2) as method
from ranked
group by record_key_a, record_key_b, combo
