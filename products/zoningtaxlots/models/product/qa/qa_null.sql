{{ config(
    materialized = 'table'
) }}

WITH new_version AS (
    SELECT * FROM {{ ref('int__zoningtaxlots') }}
),

prev_version AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_zoningtaxlots') }}
),



newnull AS (
    SELECT
        'zoning_district_1' AS field,
        COUNT(*) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.zoning_district_1 IS NULL AND b.zoning_district_1 IS NOT NULL
    UNION
    SELECT
        'zoning_district_2' AS field,
        COUNT(*) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.zoning_district_2 IS NULL AND b.zoning_district_2 IS NOT NULL
    UNION
    SELECT
        'zoning_district_3' AS field,
        COUNT(*) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.zoning_district_3 IS NULL AND b.zoning_district_3 IS NOT NULL
    UNION
    SELECT
        'zoning_district_4' AS field,
        COUNT(*) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.zoning_district_4 IS NULL AND b.zoning_district_4 IS NOT NULL
    UNION
    SELECT
        'commercial_overlay_1' AS field,
        COUNT(*) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.commercial_overlay_1 IS NULL AND b.commercial_overlay_1 IS NOT NULL
    UNION
    SELECT
        'commercial_overlay_2' AS field,
        COUNT(*) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.commercial_overlay_2 IS NULL AND b.commercial_overlay_2 IS NOT NULL
    UNION
    SELECT
        'special_district_1' AS field,
        COUNT(*) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.special_district_1 IS NULL AND b.special_district_1 IS NOT NULL
    UNION
    SELECT
        'special_district_2' AS field,
        COUNT(*) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.special_district_2 IS NULL AND b.special_district_2 IS NOT NULL
    UNION
    SELECT
        'special_district_3' AS field,
        COUNT(*) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.special_district_3 IS NULL AND b.special_district_3 IS NOT NULL
    UNION
    SELECT
        'limited_height_district' AS field,
        COUNT(*) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.limited_height_district IS NULL AND b.limited_height_district IS NOT NULL
    UNION
    SELECT
        'zoning_map_number' AS field,
        COUNT(*) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.zoning_map_number IS NULL AND b.zoning_map_number IS NOT NULL
    UNION
    SELECT
        'zoning_map_code' AS field,
        COUNT(*) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.zoning_map_code IS NULL AND b.zoning_map_code IS NOT NULL
),

newvalue AS (
    SELECT
        'zoning_district_1' AS field,
        COUNT(*) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.zoning_district_1 IS NOT NULL AND b.zoning_district_1 IS NULL
    UNION
    SELECT
        'zoning_district_2' AS field,
        COUNT(*) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.zoning_district_2 IS NOT NULL AND b.zoning_district_2 IS NULL
    UNION
    SELECT
        'zoning_district_3' AS field,
        COUNT(*) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.zoning_district_3 IS NOT NULL AND b.zoning_district_3 IS NULL
    UNION
    SELECT
        'zoning_district_4' AS field,
        COUNT(*) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.zoning_district_4 IS NOT NULL AND b.zoning_district_4 IS NULL
    UNION
    SELECT
        'commercial_overlay_1' AS field,
        COUNT(*) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.commercial_overlay_1 IS NOT NULL AND b.commercial_overlay_1 IS NULL
    UNION
    SELECT
        'commercial_overlay_2' AS field,
        COUNT(*) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.commercial_overlay_2 IS NOT NULL AND b.commercial_overlay_2 IS NULL
    UNION
    SELECT
        'special_district_1' AS field,
        COUNT(*) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.special_district_1 IS NOT NULL AND b.special_district_1 IS NULL
    UNION
    SELECT
        'special_district_2' AS field,
        COUNT(*) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.special_district_2 IS NOT NULL AND b.special_district_2 IS NULL
    UNION
    SELECT
        'special_district_3' AS field,
        COUNT(*) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.special_district_3 IS NOT NULL AND b.special_district_3 IS NULL
    UNION
    SELECT
        'limited_height_district' AS field,
        COUNT(*) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.limited_height_district IS NOT NULL AND b.limited_height_district IS NULL
    UNION
    SELECT
        'zoning_map_number' AS field,
        COUNT(*) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.zoning_map_number IS NOT NULL AND b.zoning_map_number IS NULL
    UNION
    SELECT
        'zoning_map_code' AS field,
        COUNT(*) AS count
    FROM new_version AS a
    INNER JOIN prev_version AS b
        ON a.bbl = b.bbl
    WHERE a.zoning_map_code IS NOT NULL AND b.zoning_map_code IS NULL
),

qaqc_new_nulls AS (
    SELECT
        newnull.field,
        newnull.count AS value_to_null,
        newvalue.count AS null_to_value,
        'new_version' AS version,
        'prev_version' AS version_prev
    FROM newnull LEFT JOIN newvalue
        ON newnull.field = newvalue.field
    ORDER BY value_to_null ASC, null_to_value DESC
)

SELECT * FROM qaqc_new_nulls
