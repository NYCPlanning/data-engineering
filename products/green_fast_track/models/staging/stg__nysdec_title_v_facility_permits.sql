WITH source AS (
    SELECT *
    FROM {{ source('recipe_sources', 'nysdec_title_v_facility_permits') }}
),

final AS (
    SELECT
        'title_v_permits' AS variable_type,
        permit_id AS variable_id,
        ST_Transform(geom::geometry, 2263) AS permit_geom
    FROM source
)

SELECT * FROM final
