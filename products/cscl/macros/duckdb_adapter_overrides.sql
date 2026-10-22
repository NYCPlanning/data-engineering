-- audit_helper: count(distinct a, b) isn't valid in DuckDB; use audit_helper's
-- dense_rank form instead (dispatch search order in dbt_project.yml)
{% macro duckdb___count_num_rows_in_status() -%}
    {{ audit_helper._count_num_rows_in_status_without_distinct_window_func() }}
{%- endmacro %}
