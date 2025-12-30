WITH pluto AS (
    SELECT
        *,
        zonedist1 IS null AS zd_all_null
    FROM {{ ref('stg__pluto') }}
),

-- to preserve lots with no zoning since STRING_TO_ARRAY returns an empty (zero-element) array
-- when the result of UNNEST(ARRAY[ ... ] is a string of zero length. this UNION ALL approach
-- is simpler than using joins or complicated nesting of array functions
lots_with_no_districts AS (
    SELECT
        bbl,
        null AS zd
    FROM pluto
    WHERE zd_all_null
),

districts_exploded AS (
    SELECT
        bbl,
        UNNEST(STRING_TO_ARRAY(UNNEST(ARRAY[zonedist1, zonedist2, zonedist3, zonedist4]), '/')) AS zd
    FROM pluto
    WHERE NOT zd_all_null
    GROUP BY bbl, zd
),

districts_distinct AS (
    SELECT *
    FROM districts_exploded
    UNION ALL
    SELECT *
    FROM lots_with_no_districts
),

generalized_districts AS (
    SELECT
        bbl,
        zd,
        CASE
            WHEN zd IS null THEN 'NONE'
            WHEN zd LIKE 'M%' OR zd LIKE 'C%' THEN LEFT(zd, 1)
            -- match the first group of characters that end with a number
            WHEN zd LIKE 'R%' THEN (REGEXP_MATCH(zd, '^(\w\d+)'))[1]
            ELSE zd
        END AS zoning_district_type
    FROM districts_distinct
)

SELECT * FROM generalized_districts
ORDER BY bbl, zd
