-- create a single column in order to export a text file without a delimeter or header
-- NOTE the row length may vary by borough until field lengths are tested in lion_dat
WITH lion AS (
    SELECT {{ dbt_utils.star(from=ref('lion_dat_by_field'), except=['_source_table']) }}
    FROM {{ ref('lion_dat_by_field') }}
)
SELECT rtrim(ltrim(replace(replace(lion::text, ',', ''), '"', ''), '('), ')') AS dat_column
FROM lion
