-- stg__us_parks_properties.sql

WITH clipped_to_nyc AS (
    {{ clip_to_geom(left=source("recipe_sources", "usnps_parks"), left_by="wkt") }}
),

final AS (
    SELECT
        'us_parks_properties' AS variable_type,
        objectid AS variable_id,
        geom AS raw_geom
    FROM clipped_to_nyc
)

SELECT * FROM final
