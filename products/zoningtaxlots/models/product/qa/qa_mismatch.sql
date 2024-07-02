{{ config(
    materialized = 'table'
) }}

WITH new_version AS (
    SELECT * FROM {{ ref('int__zoningtaxlots') }}
),

prev_version AS (
    SELECT * FROM {{ source('recipe_sources', 'previous_ztl') }}
),

mismatch AS (
    SELECT
        count(*) AS total,
        {{not_equal_or_null('a', 'b', 'borough_code')}} AS borough_code,
        {{not_equal_or_null('a', 'b', 'tax_block')}} AS tax_block,
        {{not_equal_or_null('a', 'b', 'tax_lot')}} AS tax_lot,
        {{not_equal_or_null('a', 'b', 'bbl')}} AS bbl,
        {{not_equal_or_null('a', 'b', 'zoning_district_1')}} AS zoning_district_1,
        {{not_equal_or_null('a', 'b', 'zoning_district_2')}} AS zoning_district_2,
        {{not_equal_or_null('a', 'b', 'zoning_district_3')}} AS zoning_district_3,
        {{not_equal_or_null('a', 'b', 'zoning_district_4')}} AS zoning_district_4,
        {{not_equal_or_null('a', 'b', 'commercial_overlay_1')}} AS commercial_overlay_1,
        {{not_equal_or_null('a', 'b', 'commercial_overlay_2')}} AS commercial_overlay_2,
        {{not_equal_or_null('a', 'b', 'special_district_1')}} AS special_district_1,
        {{not_equal_or_null('a', 'b', 'special_district_2')}} AS special_district_2,
        {{not_equal_or_null('a', 'b', 'special_district_3')}} AS special_district_3,
        {{not_equal_or_null('a', 'b', 'limited_height_district')}} AS limited_height_district,
        {{not_equal_or_null('a', 'b', 'zoning_map_number')}} AS zoning_map_number,
        {{not_equal_or_null('a', 'b', 'zoning_map_code')}} AS zoning_map_code,
        'VERSION' AS version,
        'VERSION_PREV' AS version_prev
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
)

SELECT * FROM mismatch
