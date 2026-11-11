-- Every compared pair with field levels, tier, blockers and the final decision
-- (DESIGN.md sections 7-9 and 13). Mirrors standardization/rules.py:decide.

{% set today = "cast('" ~ var('load_date') ~ "' as date)" %}
{% set gap = var('generational_gap_years') %}

with p as (
    select
        p.record_key_a,
        p.record_key_b,
        p.blocking_keys,
        a.ssn as ssn_a, b.ssn as ssn_b,
        a.dob as dob_a, b.dob as dob_b,
        a.dob_is_jan1 and b.dob_is_jan1 as dob_is_jan1,
        a.suffix as suffix_a, b.suffix as suffix_b,
        a.first_name as first_name_a, b.first_name as first_name_b,
        a.first_name_is_placeholder or a.last_name_is_placeholder as placeholder_a,
        b.first_name_is_placeholder or b.last_name_is_placeholder as placeholder_b,
        printf('%04d%02d%02d', year(a.dob), month(a.dob), day(a.dob)) as dob_digits_a,
        printf('%04d%02d%02d', year(b.dob), month(b.dob), day(b.dob)) as dob_digits_b
    from {{ ref('int_candidate_pairs') }} p
    join {{ ref('int_records_standardized') }} a on a.record_key = p.record_key_a
    join {{ ref('int_records_standardized') }} b on b.record_key = p.record_key_b
),

names as (
    select
        record_key_a,
        record_key_b,
        max(case when combo = 'first_first' then level end) as ff_level,
        max(case when combo = 'first_first' then method end) as ff_method,
        max(case when combo = 'last_last' then level end) as ll_level,
        max(case when combo = 'last_last' then method end) as ll_method,
        max(case when combo = 'first_last' then level end) as fl_level,
        max(case when combo = 'first_last' then method end) as fl_method,
        max(case when combo = 'last_first' then level end) as lf_level,
        max(case when combo = 'last_first' then method end) as lf_method
    from {{ ref('int_name_levels') }}
    group by record_key_a, record_key_b
),

levels as (
    select
        p.*,
        case
            when p.ssn_a is null or p.ssn_b is null then 'MISSING'
            when p.ssn_a = p.ssn_b then 'EXACT'
            when {{ hamming('p.ssn_a', 'p.ssn_b', 9) }} = 1 then 'NEAR'
            when {{ hamming('p.ssn_a', 'p.ssn_b', 9) }} = 2
                 and len(list_filter(range(1, 9), lambda i:
                        substr(p.ssn_a, i, 1) <> substr(p.ssn_b, i, 1)
                        and substr(p.ssn_a, i, 1) = substr(p.ssn_b, i + 1, 1)
                        and substr(p.ssn_a, i + 1, 1) = substr(p.ssn_b, i, 1))) > 0 then 'NEAR'
            else 'DIFFERENT'
        end as ssn_level,
        case
            when p.dob_a is null or p.dob_b is null then 'MISSING'
            when p.dob_a = p.dob_b then 'EXACT'
            when (year(p.dob_a) = year(p.dob_b) and month(p.dob_a) = day(p.dob_b) and day(p.dob_a) = month(p.dob_b))
              or {{ hamming('p.dob_digits_a', 'p.dob_digits_b', 8) }} = 1
              or (abs(year(p.dob_a) - year(p.dob_b)) = 1
                  and month(p.dob_a) = month(p.dob_b) and day(p.dob_a) = day(p.dob_b))
              or (month(p.dob_a) = 2 and day(p.dob_a) = 29 and year(p.dob_b) = year(p.dob_a)
                  and ((month(p.dob_b) = 2 and day(p.dob_b) = 28) or (month(p.dob_b) = 3 and day(p.dob_b) = 1)))
              or (month(p.dob_b) = 2 and day(p.dob_b) = 29 and year(p.dob_a) = year(p.dob_b)
                  and ((month(p.dob_a) = 2 and day(p.dob_a) = 28) or (month(p.dob_a) = 3 and day(p.dob_a) = 1)))
                then 'NEAR'
            else 'DIFFERENT'
        end as dob_level,
        case when p.dob_a is not null and p.dob_b is not null
             then {{ years_between('p.dob_a', 'p.dob_b') }} end as dob_gap_years,
        coalesce(n.ff_level, 'NONE') as ff_level, coalesce(n.ff_method, 'MISSING') as ff_method,
        coalesce(n.ll_level, 'NONE') as ll_level, coalesce(n.ll_method, 'MISSING') as ll_method,
        coalesce(n.fl_level, 'NONE') as fl_level, coalesce(n.fl_method, 'MISSING') as fl_method,
        coalesce(n.lf_level, 'NONE') as lf_level, coalesce(n.lf_method, 'MISSING') as lf_method
    from p
    left join names n using (record_key_a, record_key_b)
),

-- Swapped names: cross comparisons both STRONG while the direct ones are not.
named as (
    select
        *,
        not (ff_level = 'STRONG' and ll_level = 'STRONG')
            and fl_level = 'STRONG' and lf_level = 'STRONG' as names_swapped
    from levels
),

compared as (
    select
        *,
        case when names_swapped then fl_level else ff_level end as first_level,
        case when names_swapped then fl_method else ff_method end as first_method,
        case when names_swapped then lf_level else ll_level end as last_level,
        case when names_swapped then lf_method else ll_method end as last_method
    from named
),

-- Match tiers (section 8). A Jan 1 DOB is not firm enough for Tiers 2 and 4 (D-032).
tiered as (
    select
        *,
        case
            when ssn_level = 'EXACT' and dob_level = 'EXACT'
                 and 'STRONG' in (first_level, last_level) then 1
            when ssn_level = 'NEAR' and dob_level = 'EXACT' and not dob_is_jan1
                 and first_level = 'STRONG' and last_level = 'STRONG' then 2
            when ssn_level = 'EXACT' and dob_level = 'NEAR'
                 and first_level = 'STRONG' and last_level = 'STRONG' then 3
            when ssn_level = 'MISSING' and dob_level = 'EXACT' and not dob_is_jan1
                 and first_level = 'STRONG' and last_level = 'STRONG' then 4
        end as tier
    from compared
),

-- Blockers (section 9, D-035, D-036).
blocked as (
    select
        *,
        list_filter([
            case when (ssn_level in ('EXACT', 'NEAR') and dob_gap_years >= {{ gap }})
                   or (placeholder_a and dob_a <= {{ today }}
                       and {{ years_between('dob_a', today) }} < {{ var('newborn_max_age_years') }}
                       and dob_b < dob_a and {{ years_between('dob_b', 'dob_a') }} >= {{ gap }})
                   or (placeholder_b and dob_b <= {{ today }}
                       and {{ years_between('dob_b', today) }} < {{ var('newborn_max_age_years') }}
                       and dob_a < dob_b and {{ years_between('dob_a', 'dob_b') }} >= {{ gap }})
                 then 'BLOCKED_GENERATIONAL' end,
            case when suffix_a is not null and suffix_b is not null and suffix_a <> suffix_b
                 then 'BLOCKED_SUFFIX' end,
            case when dob_level = 'EXACT' and last_level = 'STRONG' and first_level = 'NONE'
                      and first_name_a is not null and first_name_b is not null
                      and ssn_level in ('NEAR', 'DIFFERENT')
                 then 'BLOCKED_TWINS' end,
            case when ssn_level = 'EXACT' and dob_level = 'DIFFERENT'
                      and first_level = 'NONE' and last_level = 'NONE'
                 then 'BLOCKED_SSN_CONFLICT' end
        ], lambda code: code is not null) as blockers
    from tiered
)

select
    {{ var('load_id') }} as load_id,
    '{{ var("profile") }}' as profile,
    record_key_a,
    record_key_b,
    blocking_keys,
    ssn_level,
    dob_level,
    dob_gap_years,
    first_level,
    first_method,
    last_level,
    last_method,
    names_swapped,
    tier,
    blockers,
    case
        when len(blockers) > 0 then blockers[1]
        when tier = 1 then 'TIER_1_EXACT_IDS'
        when tier = 2 then 'TIER_2_SSN_NEAR'
        when tier = 3 then 'TIER_3_DOB_NEAR'
        when tier = 4 then 'TIER_4_NO_SSN'
        else 'NO_MATCH_INSUFFICIENT'
    end as reason_code,
    len(blockers) = 0
        and coalesce(tier in (1, 2, 3) or (tier = 4 and {{ var('tier_4_matches') }}), false) as is_match,
    len(blockers) > 0 as cannot_link
from blocked
