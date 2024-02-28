WITH vents_raw AS (
    SELECT *
    FROM {{ source('recipe_sources', 'dcp_air_quality_vent_towers') }}
)

SELECT
    name AS variable_id,
    'vent_towers' AS variable_type,
    wkb_geometry AS raw_geom,
    ST_BUFFER(wkb_geometry, 75) AS buffer
FROM vents_raw
