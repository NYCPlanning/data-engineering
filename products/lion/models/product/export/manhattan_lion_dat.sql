WITH manhattan AS (
    SELECT *
    FROM {{ ref('lion_dat') }}
    WHERE
        "Borough" = '1'
)

-- create a single column in order to export a text file without a delimeter or header
-- NOTE the row length may vary by borough until field lengths are tested in lion_dat
SELECT rtrim(ltrim(replace(manhattan::text, ',', ''), '('), ')') AS dat_column FROM manhattan
