WITH source AS (
    SELECT *
    FROM {{ source('recipe_sources', 'nysdec_state_facility_permits') }}
),

final AS (
    SELECT
        'state_facility_permits' AS variable_type,
        permit_id AS variable_id,
        facility_name,
        ST_Transform(geom::geometry, 2263) AS permit_geom
    FROM source
)

SELECT * FROM final
