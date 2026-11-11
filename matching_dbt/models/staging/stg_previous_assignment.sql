-- Person IDs from the previous load of the same profile (empty at load 0).
{% if var("previous_output_dir") %}
select record_uid, person_id
from read_parquet('{{ var("previous_output_dir") }}/person_assignment.parquet')
{% else %}
select null::varchar as record_uid, null::varchar as person_id
where false
{% endif %}
