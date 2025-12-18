WITH word_table AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_cscl_universalword') }}
),
ranked AS (
    SELECT
        LEFT(full_spelling, 1) AS first_letter,
        ROW_NUMBER() OVER (PARTITION BY LEFT(full_name, 1) ORDER BY ogc_fid),
        *
    FROM word_table
)
SELECT
    first_letter || LPAD(row_number::TEXT, 3, '0') AS variable_name,
    full_name,
    standard_abbreviation,
    shortest_abbreviation,
    other_abbreviation,
    other_abbreviation_2,
    other_abbreviation_3,
    other_abbreviation_4,
    other_abbreviation_5,
    other_abbreviation_6,
    other_abbreviation_7,
    other_abbreviation_8,
    other_abbreviation_9,
    other_abbreviation_10
FROM ranked
