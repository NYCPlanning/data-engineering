{{ config(
    materialized = 'table'
) }}

WITH new_version AS (
    SELECT * FROM {{ ref('int__zoningtaxlots') }}
),

prev_version AS (
    SELECT * FROM {{ source('recipe_sources', 'previous_ztl') }}
),

count_old AS (
    SELECT
        b.key AS field,
        b.value::numeric AS count
    FROM (
        SELECT row_to_json(row) AS _col
        FROM (
            SELECT
                sum((zoning_district_1 IS NOT NULL)::int) AS zoning_district_1,
                sum((zoning_district_2 IS NOT NULL)::int) AS zoning_district_2,
                sum((zoning_district_3 IS NOT NULL)::int) AS zoning_district_3,
                sum((zoning_district_4 IS NOT NULL)::int) AS zoning_district_4,
                sum((commercial_overlay_1 IS NOT NULL)::int) AS commercial_overlay_1,
                sum((commercial_overlay_2 IS NOT NULL)::int) AS commercial_overlay_2,
                sum((special_district_1 IS NOT NULL)::int) AS special_district_1,
                sum((special_district_2 IS NOT NULL)::int) AS special_district_2,
                sum((special_district_3 IS NOT NULL)::int) AS special_district_3,
                sum((limited_height_district IS NOT NULL)::int) AS limited_height_district,
                sum((zoning_map_number IS NOT NULL)::int) AS zoning_map_number,
                sum((zoning_map_code IS NOT NULL)::int) AS zoning_map_code
            FROM prev_version
        ) AS row
    ) AS a
    CROSS JOIN LATERAL json_each_text(a._col) AS b
),

count_new AS (
    SELECT
        b.key AS field,
        b.value::numeric AS count
    FROM (
        SELECT row_to_json(row) AS _col
        FROM (
            SELECT
                sum((zoning_district_1 IS NOT NULL)::int) AS zoning_district_1,
                sum((zoning_district_2 IS NOT NULL)::int) AS zoning_district_2,
                sum((zoning_district_3 IS NOT NULL)::int) AS zoning_district_3,
                sum((zoning_district_4 IS NOT NULL)::int) AS zoning_district_4,
                sum((commercial_overlay_1 IS NOT NULL)::int) AS commercial_overlay_1,
                sum((commercial_overlay_2 IS NOT NULL)::int) AS commercial_overlay_2,
                sum((special_district_1 IS NOT NULL)::int) AS special_district_1,
                sum((special_district_2 IS NOT NULL)::int) AS special_district_2,
                sum((special_district_3 IS NOT NULL)::int) AS special_district_3,
                sum((limited_height_district IS NOT NULL)::int) AS limited_height_district,
                sum((zoning_map_number IS NOT NULL)::int) AS zoning_map_number,
                sum((zoning_map_code IS NOT NULL)::int) AS zoning_map_code
            FROM new_version
        ) AS row
    ) AS a
    CROSS JOIN LATERAL json_each_text(a._col) AS b
),

qaqc_frequency_change AS (
    SELECT
        a.field,
        a.count AS countold,
        b.count AS countnew
    FROM count_old AS a INNER JOIN count_new AS b ON a.field = b.field
    ORDER BY b.count - a.count DESC
)

SELECT * FROM qaqc_frequency_change
