{{ config(
    materialized = 'table'
) }}

WITH ztl AS (
    SELECT * FROM {{ ref('int__zoningtaxlots') }}
)

SELECT
    borough_code AS "Borough Code",
    trunc(tax_block::numeric) AS "Tax Block",
    tax_lot AS "Tax Lot",
    bbl AS "BBL",
    zoning_district_1 AS "Zoning District 1",
    zoning_district_2 AS "Zoning District 2",
    zoning_district_3 AS "Zoning District 3",
    zoning_district_4 AS "Zoning District 4",
    commercial_overlay_1 AS "Commercial Overlay 1",
    commercial_overlay_2 AS "Commercial Overlay 2",
    special_district_1 AS "Special District 1",
    special_district_2 AS "Special District 2",
    special_district_3 AS "Special District 3",
    limited_height_district AS "Limited Height District",
    zoning_map_number AS "Zoning Map Number",
    zoning_map_code AS "Zoning Map Code"
FROM ztl
