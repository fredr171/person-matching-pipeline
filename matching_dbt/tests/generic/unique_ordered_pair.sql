{% test unique_ordered_pair(model) %}
-- Each candidate pair appears once, with the smaller record key first.
select record_key_a, record_key_b, count(*) as copies
from {{ model }}
group by record_key_a, record_key_b
having count(*) > 1 or min(record_key_a) >= min(record_key_b)
{% endtest %}
