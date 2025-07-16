-- stg__nys_parks_properties.sql

WITH source AS (
    SELECT *
    FROM {{ source('recipe_sources', 'nysparks_parks') }}
),

filtered AS (
    SELECT
        'nys_parks_properties' AS variable_type,
        coalesce(uid || '-', '') || name AS variable_id,
        ST_Transform(wkb_geometry, 2263) AS raw_geom
    FROM source
    WHERE upper(county) IN ('BRONX', 'KINGS', 'QUEENS', 'RICHMOND', 'MANHATTAN')
)

SELECT
    variable_type,
    variable_id,
    ST_Union(raw_geom) AS raw_geom
FROM filtered
GROUP BY variable_type, variable_id
