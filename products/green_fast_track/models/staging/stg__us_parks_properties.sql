-- stg__us_parks_properties.sql

WITH source AS (
    SELECT * FROM {{ source("recipe_sources", "usnps_parks") }}
),

reprojected AS (
    SELECT
        unit_code,
        parkname,
        {{ dcp_st_transform('geom', 2263) }} AS geom
    FROM source
),

clipped_to_nyc AS (
    {{ clip_to_geom(left='reprojected', left_by='geom', left_columns=['unit_code', 'parkname']) }}
),

final AS (
    SELECT
        'us_parks_properties' AS variable_type,
        unit_code,
        parkname,
        COALESCE(unit_code || '-', '') || parkname AS variable_id,
        geom AS raw_geom
    FROM clipped_to_nyc
)

SELECT
    variable_type,
    variable_id,
    ST_UNION_AGG(raw_geom) AS raw_geom
FROM final
GROUP BY variable_type, variable_id
