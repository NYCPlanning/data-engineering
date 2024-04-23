WITH pluto AS (
    SELECT * FROM {{ ref('stg__pluto') }}
),

district_mappings AS (
    SELECT * FROM {{ ref('zoning_district_mappings') }}
),

district_categories AS (
    SELECT * FROM {{ ref('zoning_district_categories') }}
),

districts_exploded AS (
    SELECT
        bbl,
        UNNEST(ARRAY[zonedist1, zonedist2, zonedist3, zonedist4]) AS zd,
        COALESCE(zonedist1, zonedist2, zonedist3, zonedist4) IS null AS zd_all_null
    FROM pluto
),

districts_distinct AS (
    SELECT
        bbl,
        zd
    FROM districts_exploded
    WHERE (zd IS NOT null AND NOT zd_all_null) OR zd_all_null
    GROUP BY 1, 2
),

district_types AS (
    SELECT
        bbl,
        CASE
            WHEN zd IS null THEN 'NONE'
            WHEN zd LIKE 'M%' OR zd LIKE 'C%' THEN SUBSTRING(zd for 1)
            WHEN zd LIKE 'R%' THEN SPLIT_PART(zd, '-', 1)
            ELSE zd
        END AS zd_type
    FROM districts_distinct
),

districts_mapped AS (
    SELECT
        district_types.*,
        district_mappings.category_type
    FROM district_types
    LEFT JOIN district_mappings ON district_types.zd_type = district_mappings.pluto_zoning_type
),

lot_with_zoning_categories AS (
    SELECT
        bbl,
        ARRAY_AGG(DISTINCT category_type) AS category_type_array
    FROM districts_mapped
    GROUP BY 1
),

lots_with_flags AS (
    SELECT
        bbl,
        category_type_array,
        'other' = ANY(category_type_array) AS has_other,
        'c_or_m' = ANY(category_type_array) AS has_c_or_m,
        'low_res' = ANY(category_type_array) AS has_low_res
    FROM lot_with_zoning_categories
),

districts_resolved AS (
    SELECT
        bbl,
        CASE
            WHEN has_other THEN 'other'
            WHEN has_c_or_m
                THEN
                    CASE
                        WHEN has_low_res THEN 'low_res_c_or_m'
                        ELSE 'c_or_m'
                    END
            WHEN has_low_res THEN 'low_res'
            ELSE 'high_res'
        END AS category_type
    FROM lots_with_flags
),

zoning_district_flags AS (
    SELECT
        districts_resolved.bbl,
        'zoning_districts' AS variable_type,
        district_categories.label AS variable_id
    FROM districts_resolved
    LEFT JOIN district_categories ON districts_resolved.category_type = district_categories.category_type
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
