-- stg__nysdec_title_v_facility_permits.sql

WITH source AS (
    SELECT *
    FROM {{ source('ceqr_survey_sources', 'nysdec_title_v_facility_permits') }}
),

final AS (
    SELECT
        'title_v_facility_permits' AS variable,
        permit_id,
        facility_name,
        st_transform(geom::geometry, 2263) AS geom
    FROM source
)

SELECT * FROM final
