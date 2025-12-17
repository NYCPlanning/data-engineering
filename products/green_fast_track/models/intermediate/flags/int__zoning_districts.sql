WITH pluto AS (
    SELECT * FROM {{ ref('stg__pluto') }}
),

districts_exploded AS (
    SELECT
        bbl,
        UNNEST(ARRAY[zonedist1, zonedist2, zonedist3, zonedist4]) AS zd,
        zonedist1 IS null AS zd_all_null
    FROM pluto
),

districts_distinct AS (
    SELECT
        bbl,
        zd
    FROM districts_exploded
    WHERE (zd IS NOT null AND NOT zd_all_null) OR zd_all_null
    GROUP BY bbl, zd
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
