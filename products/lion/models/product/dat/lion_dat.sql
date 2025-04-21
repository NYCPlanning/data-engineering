-- create a single column in order to export a text file without a delimeter or header
-- NOTE the row length may vary by borough until field lengths are tested in lion_dat
WITH lion AS (
    SELECT * FROM {{ ref('lion_dat_fields') }}
),
SELECT rtrim(ltrim(replace(lion::text, ',', ''), '('), ')') AS dat_column
FROM lion
