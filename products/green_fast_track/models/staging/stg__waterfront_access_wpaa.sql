-- stg__waterfront_access_wpaa.sql

WITH source AS (
    SELECT *
    FROM {{ source('recipe_sources', 'dcp_waterfront_access_map_wpaa') }}
),

filtered AS (
    SELECT
        'waterfront_access_wpaa' AS variable_type,
        wpaa_id || '-' || name AS variable_id,
        ST_TRANSFORM(wkb_geometry, 2263) AS raw_geom
    FROM source
    WHERE UPPER(status) != 'CLOSED'
)

SELECT * FROM filtered
