-- Every pair of comparison forms for each candidate pair and each of the four
-- field pairings: first-first, last-last, and the two cross pairings used to
-- detect swapped names (DESIGN.md section 7, D-030).

{% set combos = [
    ('first_first', 'first', 'first'),
    ('last_last', 'last', 'last'),
    ('first_last', 'first', 'last'),
    ('last_first', 'last', 'first'),
] %}

with p as (
    select
        p.record_key_a,
        p.record_key_b,
        a.first_name_forms as a_first,
        a.last_name_forms as a_last,
        b.first_name_forms as b_first,
        b.last_name_forms as b_last
    from {{ ref('int_candidate_pairs') }} p
    join {{ ref('int_records_standardized') }} a on a.record_key = p.record_key_a
    join {{ ref('int_records_standardized') }} b on b.record_key = p.record_key_b
)

{% for combo, field_a, field_b in combos %}
select p.record_key_a, p.record_key_b, '{{ combo }}' as combo, x.form_a, y.form_b
from p, unnest(p.a_{{ field_a }}) as x(form_a), unnest(p.b_{{ field_b }}) as y(form_b)
{% if not loop.last %}union all{% endif %}
{% endfor %}
