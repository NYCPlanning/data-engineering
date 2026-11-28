{{ config(materialized='table') }}

WITH
libraries AS (
    SELECT
        library_name,
        borough,
        ST_TRANSFORM(wkb_geometry, 2263) AS geom
    FROM {{ ref('libraries') }}
),

-- "Parks" = places you'd go to spend time outdoors (no plazas, strips, gardens or operational lots)
parks AS (
    SELECT
        space_name AS park_name,
        ST_TRANSFORM(wkb_geometry, 2263) AS geom
    FROM {{ ref('stg_dpr_parksproperties') }}
    WHERE typecategory IN (
        'Flagship Park',
        'Community Park',
        'Neighborhood Park',
        'Playground',
        'Jointly Operated Playground',
        'Nature Area',
        'Recreational Field/Courts',
        'Historic House Park'
    )
)

SELECT
    libraries.library_name,
    libraries.borough,
    nearest.park_name AS nearest_park,
    nearest.distance_ft AS nearest_park_distance_ft
FROM libraries
CROSS JOIN LATERAL (
    SELECT
        parks.park_name,
        ROUND(ST_DISTANCE(libraries.geom, parks.geom)) AS distance_ft
    FROM parks
    ORDER BY distance_ft
    LIMIT 1
) AS nearest
