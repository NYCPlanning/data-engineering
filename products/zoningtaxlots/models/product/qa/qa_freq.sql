{{ config(
    materialized = 'table'
) }}

WITH new_version AS (
    SELECT * FROM {{ ref('int__zoningtaxlots') }}
),

freq AS (
    SELECT
        count(borough_code) AS borough_code,
        count(tax_block) AS tax_block,
        count(tax_lot) AS tax_lot,
        count(bbl) AS bbl,
        count(zoning_district_1) AS zoning_district_1,
        count(zoning_district_2) AS zoning_district_2,
        count(zoning_district_3) AS zoning_district_3,
        count(zoning_district_4) AS zoning_district_4,
        count(commercial_overlay_1) AS commercial_overlay_1,
        count(commercial_overlay_2) AS commercial_overlay_2,
        count(special_district_1) AS special_district_1,
        count(special_district_2) AS special_district_2,
        count(special_district_3) AS special_district_3,
        count(limited_height_district) AS limited_height_district,
        count(zoning_map_number) AS zoning_map_number,
        count(zoning_map_code) AS zoning_map_code,
        count(area) AS area,
        '{{env_var('VERSION')}}'::text AS version
    FROM new_version
)

SELECT * FROM freq
