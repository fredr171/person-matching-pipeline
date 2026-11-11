-- Blocking key values per record (DESIGN.md section 6, D-040); mirrors
-- standardization/blocking.py:key_values. Records lacking a key's fields get no value.

with r as (
    select * from {{ ref('int_records_standardized') }}
),

all_keys as (
    select record_key, 'ssn' as key_name, ssn as key_value
    from r where ssn is not null

    union all
    select record_key, 'dob', {{ iso_date('dob') }}
    from r where dob is not null

    union all
    select record_key, 'ssn_sorted_digits_birth_year',
           array_to_string(list_sort(string_split(ssn, '')), '') || '|' || {{ year_text('dob') }}
    from r where ssn is not null and dob is not null

    union all
    select record_key, 'ssn_first5_birth_year', left(ssn, 5) || '|' || {{ year_text('dob') }}
    from r where ssn is not null and dob is not null

    union all
    select record_key, 'last_phonetic_first_initial_birth_year',
           codes[1] || '|' || left(first_name, 1) || '|' || {{ year_text('dob') }}
    from (
        select *,
               pmp_double_metaphone(replace(replace(replace(last_name, ' ', ''), '-', ''), '''', '')) as codes
        from r
        where last_name is not null and first_name is not null and dob is not null
    )
    where len(codes) > 0
)

select *
from all_keys
where key_name in ({% for key in var('blocking_keys') %}'{{ key }}'{% if not loop.last %}, {% endif %}{% endfor %})
