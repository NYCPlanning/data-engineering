-- DuckDB build: models keep their Postgres `indexes` configs (gist etc.) while both
-- engines exist; DuckDB scans columnar data and has no gist, so skip index creation.
{% macro duckdb__get_create_index_sql(relation, index_dict) -%}
{%- endmacro %}
