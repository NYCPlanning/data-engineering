-- stg__nysdec_title_v_facility_permits.sql

WITH source AS (
    SELECT *
    FROM {{ source('ceqr_survey_sources', 'nysdec_title_v_facility_permits') }}
),

final AS (
    SELECT
        'title_v_facility_permits' AS variable_type,
        permit_id,
        st_transform(geom::geometry, 2263) AS permit_geom
    FROM source
)

SELECT * FROM final
