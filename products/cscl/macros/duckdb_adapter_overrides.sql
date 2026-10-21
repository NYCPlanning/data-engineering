-- Models carry `indexes` configs (gist etc.); DuckDB scans columnar data and has no
-- gist, so index creation is skipped.
{% macro duckdb__get_create_index_sql(relation, index_dict) -%}
{%- endmacro %}

-- audit_helper: count(distinct a, b) isn't valid in DuckDB; use the dense_rank form
-- audit_helper already uses on Postgres (dispatch search order in dbt_project.yml)
{% macro duckdb___count_num_rows_in_status() -%}
    {{ audit_helper._count_num_rows_in_status_without_distinct_window_func() }}
{%- endmacro %}
