WITH vents_raw AS (
    SELECT *
    FROM {{ source('recipe_sources', 'dcp_air_quality_vent_towers') }}
),

reprojected AS (
    SELECT
        name,
        {{ dcp_st_transform('wkb_geometry', 2263) }} AS geom
    FROM vents_raw
)

SELECT
    'vent_tower' AS flag_id_field_name,
    'vent_towers' AS variable_type,
    name AS variable_id,
    geom AS raw_geom,
    ST_BUFFER(geom, 75) AS buffer_geom
FROM reprojected
