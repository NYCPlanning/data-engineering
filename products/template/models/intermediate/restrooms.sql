{{ config(materialized='table') }}

WITH
restrooms AS (SELECT * FROM {{ ref('stg_moo_public_restrooms') }}),

boroughs AS (SELECT * FROM {{ ref('stg_dcp_boroboundaries_wi') }}),

operational AS (
    SELECT *
    FROM restrooms
    WHERE status = 'Operational'
),

with_borough AS (
    SELECT
        r.restroom_name,
        b.borough,
        r.location_type,
        r.operator,
        r.hours_of_operation,
        r.accessibility,
        r.wkb_geometry
    FROM operational AS r
    LEFT JOIN boroughs AS b
        ON ST_WITHIN(r.wkb_geometry, b.wkb_geometry)
),

-- The source lists a few restrooms twice at the same spot. Keep one.
deduplicated AS (
    SELECT DISTINCT ON (restroom_name, wkb_geometry) *
    FROM with_borough
    ORDER BY restroom_name, wkb_geometry
),

-- Some parks have several restrooms with the same name. Number them
-- west to east, so each name is unique within its borough.
numbered AS (
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY restroom_name, borough) AS name_count,
        ROW_NUMBER() OVER (
            PARTITION BY restroom_name, borough
            ORDER BY ST_X(wkb_geometry), ST_Y(wkb_geometry)
        ) AS name_number
    FROM deduplicated
),

final AS (
    SELECT
        CASE
            WHEN name_count > 1 THEN restroom_name || ' (' || name_number || ')'
            ELSE restroom_name
        END AS restroom_name,
        borough,
        location_type,
        operator,
        hours_of_operation,
        accessibility,
        wkb_geometry
    FROM numbered
)

SELECT * FROM final
