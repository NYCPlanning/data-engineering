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
{#- Concatenates the columns: commas and quotes removed from each value (the macro's
    original row-to-text form), NULL as empty. -#}
{%- set export_columns = [] -%}
{%- for col in columns if col.name not in all_excludes -%}
    {%- do export_columns.append(col.name) -%}
{%- endfor %}
SELECT rtrim(ltrim(concat(
    {%- for col in export_columns %}
    replace(replace(coalesce("{{ col }}"::VARCHAR, ''), ',', ''), '"', ''){{ "," if not loop.last }}
    {%- endfor %}
), '('), ')') AS dat_column
FROM {{ model_relation }}
{%- endmacro %}
