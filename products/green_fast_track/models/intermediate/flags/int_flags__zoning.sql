WITH pluto AS (
    SELECT * FROM {{ ref('stg__pluto') }}
),

zoning_districts AS (
    SELECT * FROM {{ ref('int__zoning_districts') }}
),

district_mappings AS (
    SELECT * FROM {{ ref('zoning_district_mappings') }}
),

district_categories AS (
    SELECT * FROM {{ ref('zoning_district_categories') }}
),

districts_mapped AS (
    SELECT
        zoning_districts.bbl,
        zoning_districts.zoning_district_type,
        district_mappings.category_type
    FROM zoning_districts
    LEFT JOIN district_mappings ON zoning_districts.zoning_district_type = district_mappings.zoning_district_type
),

lot_with_zoning_categories AS (
    SELECT
        bbl,
        array_agg(DISTINCT category_type) AS category_type_array
    FROM districts_mapped
    GROUP BY bbl
),

lots_with_flags AS (
    SELECT
        bbl,
        category_type_array,
        'other' = any(category_type_array) AS has_other,
        'c_or_m' = any(category_type_array) AS has_c_or_m,
        'low_res' = any(category_type_array) AS has_low_res
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
        'zoning_district' AS flag_id_field_name,
        'zoning_districts' AS variable_type,
        district_categories.label AS variable_id
    FROM districts_resolved
    LEFT JOIN district_categories ON districts_resolved.category_type = district_categories.category_type
),

special_district_flags AS (
    SELECT
        bbl,
        'special_coastal_risk' AS flag_id_field_name,
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
    flag_id_field_name,
    variable_type,
    variable_id
FROM zoning_district_flags
UNION ALL
SELECT
    bbl,
    flag_id_field_name,
    variable_type,
    variable_id
FROM special_district_flags
