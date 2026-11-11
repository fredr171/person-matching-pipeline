-- The first unused person ID sequence number after the previous load (1 at load 0).
{% if var("previous_output_dir") %}
select next_sequence
from read_parquet('{{ var("previous_output_dir") }}/id_state.parquet')
{% else %}
select 1::bigint as next_sequence
{% endif %}
