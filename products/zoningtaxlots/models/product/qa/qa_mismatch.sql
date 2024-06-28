{{ config(
    materialized = 'table'
) }}

WITH new_version AS (
    SELECT * FROM {{ ref('int__zoningtaxlots') }}
),

prev_version AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_zoningtaxlots') }}
),

mismatch AS(
    SELECT
        count(*) AS total,
        sum(
            (a.borough_code != b.borough_code OR (a.borough_code IS NULL AND b.borough_code IS NOT NULL))::integer
        ) AS borough_code,
        sum((a.tax_block != b.tax_block OR (a.tax_block IS NULL AND b.tax_block IS NOT NULL))::integer) AS tax_block,
        sum((a.tax_lot != b.tax_lot OR (a.tax_lot IS NULL AND b.tax_lot IS NOT NULL))::integer) AS tax_lot,
        sum((a.bbl != b.bbl OR (a.bbl IS NULL AND b.bbl IS NOT NULL))::integer) AS bbl,
        sum(
            (
                a.zoning_district_1 != b.zoning_district_1 OR (a.zoning_district_1 IS NULL AND b.zoning_district_1 IS NOT NULL)
            )::integer
        ) AS zoning_district_1,
        sum(
            (
                a.zoning_district_2 != b.zoning_district_2 OR (a.zoning_district_2 IS NULL AND b.zoning_district_2 IS NOT NULL)
            )::integer
        ) AS zoning_district_2,
        sum(
            (
                a.zoning_district_3 != b.zoning_district_3 OR (a.zoning_district_3 IS NULL AND b.zoning_district_3 IS NOT NULL)
            )::integer
        ) AS zoning_district_3,
        sum(
            (
                a.zoning_district_4 != b.zoning_district_4 OR (a.zoning_district_4 IS NULL AND b.zoning_district_4 IS NOT NULL)
            )::integer
        ) AS zoning_district_4,
        sum(
            (
                a.commercial_overlay_1 != b.commercial_overlay_1
                OR (a.commercial_overlay_1 IS NULL AND b.commercial_overlay_1 IS NOT NULL)
            )::integer
        ) AS commercial_overlay_1,
        sum(
            (
                a.commercial_overlay_2 != b.commercial_overlay_2
                OR (a.commercial_overlay_2 IS NULL AND b.commercial_overlay_2 IS NOT NULL)
            )::integer
        ) AS commercial_overlay_2,
        sum(
            (
                a.special_district_1 != b.special_district_1
                OR (a.special_district_1 IS NULL AND b.special_district_1 IS NOT NULL)
            )::integer
        ) AS special_district_1,
        sum(
            (
                a.special_district_2 != b.special_district_2
                OR (a.special_district_2 IS NULL AND b.special_district_2 IS NOT NULL)
            )::integer
        ) AS special_district_2,
        sum(
            (
                a.special_district_3 != b.special_district_3
                OR (a.special_district_3 IS NULL AND b.special_district_3 IS NOT NULL)
            )::integer
        ) AS special_district_3,
        sum(
            (
                a.limited_height_district != b.limited_height_district
                OR (a.limited_height_district IS NULL AND b.limited_height_district IS NOT NULL)
            )::integer
        ) AS limited_height_district,
        sum(
            (
                a.zoning_map_number != b.zoning_map_number OR (a.zoning_map_number IS NULL AND b.zoning_map_number IS NOT NULL)
            )::integer
        ) AS zoning_map_number,
        sum(
            (a.zoning_map_code != b.zoning_map_code OR (a.zoning_map_code IS NULL AND b.zoning_map_code IS NOT NULL))::integer
        ) AS zoning_map_code,
        'new_version' AS version,
        'prev_version' AS version_prev
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
)

SELECT * FROM mismatch
