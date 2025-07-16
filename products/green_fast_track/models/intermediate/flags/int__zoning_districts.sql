WITH pluto AS (
    SELECT * FROM {{ ref('stg__pluto') }}
),

districts_exploded AS (
    SELECT
        bbl,
        unnest(ARRAY[zonedist1, zonedist2, zonedist3, zonedist4]) AS zd,
        zonedist1 IS NULL AS zd_all_null
    FROM pluto
),

districts_distinct AS (
    SELECT
        bbl,
        zd
    FROM districts_exploded
    WHERE (zd IS NOT NULL AND NOT zd_all_null) OR zd_all_null
    GROUP BY bbl, zd
),

generalized_districts AS (
    SELECT
        bbl,
        CASE
            WHEN zd IS NULL THEN 'NONE'
            WHEN zd LIKE 'M%' OR zd LIKE 'C%' THEN left(zd, 1)
            -- match the first group of characters that end with a number
            WHEN zd LIKE 'R%' THEN (regexp_match(zd, '^(\w\d+)'))[1]
            ELSE zd
        END AS zoning_district_type
    FROM districts_distinct
)

SELECT * FROM generalized_districts
GROUP BY bbl, zoning_district_type
ORDER BY bbl, zoning_district_type
