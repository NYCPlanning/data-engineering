-- stg__waterfront_access_wpaa.sql

WITH source AS (
    SELECT *
    FROM {{ source('recipe_sources', 'dcp_waterfront_access_map_wpaa') }}
),

final AS (
    SELECT
        'waterfront_access_wpaa' AS variable_type,
        objectid AS variable_id,
        st_transform(wkb_geometry, 2263) AS raw_geom
    FROM source

)

SELECT * FROM final
