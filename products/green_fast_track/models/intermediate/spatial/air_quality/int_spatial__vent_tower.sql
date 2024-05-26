WITH vents_raw AS (
    SELECT *
    FROM {{ source('recipe_sources', 'dcp_air_quality_vent_towers') }}
)

SELECT
    'vent_tower' AS flag_variable_type,
    'vent_towers' AS variable_type,
    name AS variable_id,
    wkb_geometry AS raw_geom,
    ST_BUFFER(wkb_geometry, 75) AS buffer_geom
FROM vents_raw
