-- stg__nys_parks_properties.sql

WITH source AS (
    SELECT *
    FROM {{ source('recipe_sources', 'nysparks_parks_polygons') }}
),

final AS (
    SELECT
        'nys_parks_properties' AS variable_type,
        uid,
        name,
        st_transform(wkb_geometry, 2263) AS raw_geom
    FROM source
    WHERE upper(county) IN ('BRONX', 'KINGS', 'QUEENS', 'RICHMOND', 'MANHATTAN')
)

SELECT * FROM final
