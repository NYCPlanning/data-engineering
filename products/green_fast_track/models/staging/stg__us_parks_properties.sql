-- stg__us_parks_properties.sql

WITH source AS (
    SELECT * FROM {{ source("recipe_sources", "usnps_parks") }}
),

reprojected AS (
    SELECT
        *,
        ST_Transform(wkt, 2263) AS geom
    FROM source
),

clipped_to_nyc AS (
    {{ clip_to_geom(left='reprojected', left_by='geom', left_columns=['gnis_id', 'parkname']) }}
),

final AS (
    SELECT
        'us_parks_properties' AS variable_type,
        gnis_id,
        parkname,
        coalesce(gnis_id || '-', '') || parkname AS variable_id,
        geom AS raw_geom
    FROM clipped_to_nyc
)

SELECT
    variable_type,
    variable_id,
    ST_Union(raw_geom) AS raw_geom
FROM final
GROUP BY variable_type, variable_id
