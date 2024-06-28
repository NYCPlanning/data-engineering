{{ config(
    materialized = 'table'
) }}

WITH new_version AS (
    SELECT * FROM {{ ref('int__zoningtaxlots') }}
),

prev_version AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_zoningtaxlots') }}
),

pivot_mismatch AS (
    SELECT
        'zoning_district_1' AS field,
        count(nullif(a.zoning_district_1 = b.zoning_district_1, true)) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.zoning_district_1 IS NOT null
    UNION
    SELECT
        'zoning_district_2' AS field,
        count(nullif(a.zoning_district_2 = b.zoning_district_2, true)) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.zoning_district_2 IS NOT null
    UNION
    SELECT
        'zoning_district_3' AS field,
        count(nullif(a.zoning_district_3 = b.zoning_district_3, true)) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.zoning_district_3 IS NOT null
    UNION
    SELECT
        'zoning_district_4' AS field,
        count(nullif(a.zoning_district_4 = b.zoning_district_4, true)) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.zoning_district_4 IS NOT null
    UNION
    SELECT
        'commercial_overlay_1' AS field,
        count(nullif(a.commercial_overlay_1 = b.commercial_overlay_1, true)) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.commercial_overlay_1 IS NOT null
    UNION
    SELECT
        'commercial_overlay_2' AS field,
        count(nullif(a.commercial_overlay_2 = b.commercial_overlay_2, true)) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.commercial_overlay_2 IS NOT null
    UNION
    SELECT
        'special_district_1' AS field,
        count(nullif(a.special_district_1 = b.special_district_1, true)) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.special_district_1 IS NOT null
    UNION
    SELECT
        'special_district_2' AS field,
        count(nullif(a.special_district_2 = b.special_district_2, true)) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.special_district_2 IS NOT null
    UNION
    SELECT
        'special_district_3' AS field,
        count(nullif(a.special_district_3 = b.special_district_3, true)) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.special_district_3 IS NOT null
    UNION
    SELECT
        'limited_height_district' AS field,
        count(nullif(a.limited_height_district = b.limited_height_district, true)) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.limited_height_district IS NOT null
    UNION
    SELECT
        'zoning_map_number' AS field,
        count(nullif(a.zoning_map_number = b.zoning_map_number, true)) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.zoning_map_number IS NOT null
    UNION
    SELECT
        'zoning_map_code' AS field,
        count(nullif(a.zoning_map_code = b.zoning_map_code, true)) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.zoning_map_code IS NOT null
),

countall AS (
    SELECT count(*) AS countall
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
),

comp AS (
    SELECT
        a.field,
        a.count,
        round((a.count / b.countall) * 100, 2) AS percent
    FROM pivot_mismatch AS a, countall AS b
    ORDER BY percent DESC
)

SELECT * FROM comp
