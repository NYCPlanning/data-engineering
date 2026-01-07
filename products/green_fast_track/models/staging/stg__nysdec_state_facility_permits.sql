WITH source AS (
    SELECT * FROM {{ source('recipe_sources', 'nysdec_state_facility_permits') }}
),
deduplicated AS (
    SELECT DISTINCT
        dec_id,
        facility_name,
        geom
    FROM source
),
final AS (
    SELECT
        'state_facility_permits' AS variable_type,
        dec_id AS variable_id,
        facility_name,
        ST_TRANSFORM(geom::geometry, 2263) AS permit_geom
    FROM deduplicated
)
SELECT * FROM final
