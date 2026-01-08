WITH source AS (
    SELECT * FROM {{ source('recipe_sources', 'nysdec_title_v_facility_permits') }}
),
deduplicated AS (
    SELECT DISTINCT
        dec_id,
        facility_name,
        year,
        siccode,
        geom
    FROM source
),
final AS (
    SELECT
        'title_v_permits' AS variable_type,
        dec_id AS variable_id,
        st_transform(geom::geometry, 2263) AS permit_geom
    FROM deduplicated
)
SELECT * FROM final
