WITH clipped_to_nyc AS (
    {{ clip_to_geom(left=source("recipe_sources", "nysdec_freshwater_wetlands_checkzones"), left_by="wkb_geometry") }}
)

SELECT
    'wetland_checkzone' AS flag_id_field_name,
    'wetlands_checkzones' AS variable_type,
    objectid AS variable_id,
    ST_Multi(geom) AS raw_geom,
    NULL AS buffer
FROM clipped_to_nyc
