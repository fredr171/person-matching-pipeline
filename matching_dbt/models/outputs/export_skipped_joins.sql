-- Written to {{ var('output_dir') }}/skipped_joins.parquet (DESIGN.md section 13), rows in a
-- fixed order. A COPY post-hook is used rather than dbt-duckdb's external
-- materialization, which writes an all-null row when a table is empty.
{{ config(post_hook="copy (select * from {{ this }} order by all) to '" ~ var('output_dir') ~ "/skipped_joins.parquet' (format parquet)") }}
select * from {{ ref('skipped_joins') }}
