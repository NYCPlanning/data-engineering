WITH pluto AS (
    SELECT * FROM {{ ref('stg__pluto') }}
),

zoning_districts_exploded AS (
    SELECT
        bbl,
        UNNEST(ARRAY[zonedist1, zonedist2, zonedist3, zonedist4]) AS zd
    FROM pluto
),

zoning_districts_split_exploded AS (
    SELECT
        bbl,
        UNNEST(STRING_TO_ARRAY(zd, '/')) AS zd
    FROM zoning_districts_exploded
    WHERE zd IS NOT NULL
),

zoning_districts_preprocessed AS (
    SELECT
        bbl,
        (REGEXP_MATCH(zd, '^(\w\d+)(?:[^\d].*)?$'))[1] AS zd,
        zd LIKE 'M%' OR zd LIKE 'C%' AS cmflag
    FROM zoning_districts_split_exploded
),

zoning_districts_regrouped AS (
    SELECT
        bbl,
        ARRAY_AGG(zd) <@ ARRAY['R5', 'R6', 'R7', 'R8', 'R9', 'R10'] AS r5_r10,
        ARRAY_AGG(zd) && ARRAY['R1', 'R2', 'R3', 'R4'] AS r1_r4,
        BOOL_AND(cmflag) AS c_m
    FROM zoning_districts_preprocessed
    GROUP BY bbl
),

zoning_district_flags AS (
    SELECT
        bbl,
        'zoning_districts' AS variable_type,
        CASE
            WHEN r5_r10 THEN 'R5-R10'
            WHEN r1_r4 THEN 'R1-R4'
            WHEN c_m THEN 'C or M'
        END AS variable_id
    FROM zoning_districts_regrouped
    WHERE r5_r10 OR r1_r4 OR c_m
),

special_district_flags AS (
    SELECT
        bbl,
        'special_coastal_risk_districts' AS variable_type,
        CASE
            WHEN spdist1 LIKE 'CR%' THEN spdist1
            WHEN spdist2 LIKE 'CR%' THEN spdist2
            WHEN spdist3 LIKE 'CR%' THEN spdist3
        END AS variable_id
    FROM pluto
    WHERE
        spdist1 LIKE 'CR%'
        OR spdist2 LIKE 'CR%'
        OR spdist3 LIKE 'CR%'
)

SELECT
    bbl,
    variable_type,
    variable_id
FROM zoning_district_flags
UNION ALL
SELECT
    bbl,
    variable_type,
    variable_id
FROM special_district_flags
