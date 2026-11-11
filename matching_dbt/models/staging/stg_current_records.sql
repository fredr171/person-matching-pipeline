-- Live source records after ingestion for the load being matched
-- (data/ingested/load_NNNN/current_records.parquet, DESIGN.md section 4).
select
    source_system,
    source_record_id,
    record_life,
    first_name,
    last_name,
    dob,
    ssn,
    gender
from read_parquet('{{ var("current_records_path") }}')
