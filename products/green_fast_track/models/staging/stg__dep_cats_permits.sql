WITH source AS (
    SELECT * FROM {{ source('recipe_sources', 'dep_cats_permits') }}
),

final AS (
    SELECT
        'cats_permits' AS variable_type,
        applicationid AS variable_id,
        status,
        ST_Transform(geom::geometry, 2263) AS permit_geom
    FROM source
)

SELECT * FROM final
