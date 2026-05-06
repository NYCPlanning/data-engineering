/*
This macro creates a single text column representation of a model's rows
for export as a .dat file without delimiter or header.

Currently, it removes commas, quotes, and parentheses from the text representation,
meaning it will not behave as expected if those characters are part of the data.

It's recommended to add a column length test in the model's test suite to ensure
expected behavior.

Note: Automatically excludes columns starting with underscore (_) as these are
typically internal/helper columns not meant for export.
*/

{% macro select_rows_as_text(model, exclude=[]) -%}
{%- set model_relation = ref(model) -%}
{%- set columns = adapter.get_columns_in_relation(model_relation) -%}
{%- set underscore_cols = [] -%}
{%- for col in columns -%}
    {%- if col.name.startswith('_') -%}
        {%- do underscore_cols.append(col.name) -%}
    {%- endif -%}
{%- endfor -%}
{%- set all_excludes = underscore_cols + exclude -%}
WITH source AS (
    SELECT {{ dbt_utils.star(from=model_relation, except=all_excludes) }}
    FROM {{ model_relation }}
)
SELECT rtrim(ltrim(replace(replace(source::text, ',', ''), '"', ''), '('), ')') AS dat_column
FROM source
{%- endmacro %}
