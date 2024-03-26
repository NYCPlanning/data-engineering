-- stg__us_parks_properties.sql

WITH source AS (
    SELECT * FROM {{ source("recipe_sources", "usnps_parks") }}
),

reprojected AS (
    SELECT
        *,
        st_transform(wkt, 2263) AS geom
    FROM source
),

clipped_to_nyc AS (
    {{ clip_to_geom(left='reprojected', left_by='geom', left_columns=['objectid']) }}
),

final AS (
    SELECT
        'us_parks_properties' AS variable_type,
        objectid AS variable_id,
        geom AS raw_geom
    FROM clipped_to_nyc
)

SELECT * FROM final
