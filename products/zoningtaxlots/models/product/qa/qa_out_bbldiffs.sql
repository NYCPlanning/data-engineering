{{ config(
    materialized = 'table'
) }}

WITH new_version AS (
    SELECT * FROM {{ ref('int__zoningtaxlots') }}
),

prev_version AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_zoningtaxlots') }}
),

bbldiffs AS (
    SELECT
        a.dtm_id,
        a.borough_code,
        a.tax_block,
        a.tax_lot,
        a.bbl AS bblnew,
        a.zoning_district_1 AS zd1new,
        a.zoning_district_2 AS zd2new,
        a.zoning_district_3 AS zd3new,
        a.zoning_district_4 AS zd4new,
        a.commercial_overlay_1 AS co1new,
        a.commercial_overlay_2 AS co2new,
        a.special_district_1 AS sd1new,
        a.special_district_2 AS sd2new,
        a.special_district_3 AS sd3new,
        a.limited_height_district AS lhdnew,
        a.zoning_map_number AS zmnnew,
        a.zoning_map_code AS zmcnew,
        a.area,
        a.inzonechange,
        b.bbl AS bblprev,
        b.zoning_district_1 AS zd1prev,
        b.zoning_district_2 AS zd2prev,
        b.zoning_district_3 AS zd3prev,
        b.zoning_district_4 AS zd4prev,
        b.commercial_overlay_1 AS co1prev,
        b.commercial_overlay_2 AS co2prev,
        b.special_district_1 AS sd1prev,
        b.special_district_2 AS sd2prev,
        b.special_district_3 AS sd3prev,
        b.limited_height_district AS lhdprev,
        b.zoning_map_number AS zmnprev,
        b.zoning_map_code AS zmcprev
    FROM new_version AS a, prev_version AS b
    WHERE
        a.bbl = b.bbl
        AND a.borough_code = b.borough_code
        AND a.tax_block = b.tax_block
        AND a.tax_lot = b.tax_lot
        AND (
            a.zoning_district_1 != b.zoning_district_1
            OR a.zoning_district_2 != b.zoning_district_2
            OR a.zoning_district_3 != b.zoning_district_3
            OR a.zoning_district_4 != b.zoning_district_4
            OR a.commercial_overlay_1 != b.commercial_overlay_1
            OR a.commercial_overlay_2 != b.commercial_overlay_2
            OR a.special_district_1 != b.special_district_1
            OR a.special_district_2 != b.special_district_2
            OR a.special_district_3 != b.special_district_3
            OR a.limited_height_district != b.limited_height_district
            OR a.zoning_map_number != b.zoning_map_number
            OR a.zoning_map_code != b.zoning_map_code
            OR a.zoning_district_1 IS NULL AND b.zoning_district_1 IS NOT NULL
            OR a.zoning_district_2 IS NULL AND b.zoning_district_2 IS NOT NULL
            OR a.zoning_district_3 IS NULL AND b.zoning_district_3 IS NOT NULL
            OR a.zoning_district_4 IS NULL AND b.zoning_district_4 IS NOT NULL
            OR a.commercial_overlay_1 IS NULL AND b.commercial_overlay_1 IS NOT NULL
            OR a.commercial_overlay_2 IS NULL AND b.commercial_overlay_2 IS NOT NULL
            OR a.special_district_1 IS NULL AND b.special_district_1 IS NOT NULL
            OR a.special_district_2 IS NULL AND b.special_district_2 IS NOT NULL
            OR a.special_district_3 IS NULL AND b.special_district_3 IS NOT NULL
            OR a.zoning_map_number IS NULL AND b.zoning_map_number IS NOT NULL
            OR a.zoning_map_code IS NULL AND b.zoning_map_code IS NOT NULL
            OR b.zoning_district_1 IS NULL AND a.zoning_district_1 IS NOT NULL
            OR b.zoning_district_2 IS NULL AND a.zoning_district_2 IS NOT NULL
            OR b.zoning_district_3 IS NULL AND a.zoning_district_3 IS NOT NULL
            OR b.zoning_district_4 IS NULL AND a.zoning_district_4 IS NOT NULL
            OR b.commercial_overlay_1 IS NULL AND a.commercial_overlay_1 IS NOT NULL
            OR b.commercial_overlay_2 IS NULL AND a.commercial_overlay_2 IS NOT NULL
            OR b.special_district_1 IS NULL AND a.special_district_1 IS NOT NULL
            OR b.special_district_2 IS NULL AND a.special_district_2 IS NOT NULL
            OR b.special_district_3 IS NULL AND a.special_district_3 IS NOT NULL
            OR b.zoning_map_number IS NULL AND a.zoning_map_number IS NOT NULL
            OR b.zoning_map_code IS NULL AND a.zoning_map_code IS NOT NULL
        )
)

SELECT * FROM bbldiffs
