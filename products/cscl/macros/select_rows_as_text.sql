/* 
This macro creates a single text column representation of a model's rows 
for export as a .dat file without delimiter or header.

Currently, it removes commas, quotes, and parentheses from the text representation,
meaning it will not behave as expected if those characters are part of the data.

It's recommended to add a column length test in the model's test suite to ensure
expected behavior.
*/

{% macro select_rows_as_text(model, exclude=[]) -%}
WITH source AS (
    SELECT {{ dbt_utils.star(from=ref(model), except=exclude) }}
    FROM {{ ref(model) }}
)
SELECT rtrim(ltrim(replace(replace(source::text, ',', ''), '"', ''), '('), ')') AS dat_column
FROM source
{%- endmacro %}
