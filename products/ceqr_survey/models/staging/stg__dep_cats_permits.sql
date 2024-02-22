WITH source AS (
    SELECT * FROM {{ source('ceqr_survey_sources', 'dep_cats_permits') }}
),

final AS (
    SELECT
        'cats_permits' AS variable_type,
        applicationid AS variable_id,
        status,
        st_transform(geom::geometry, 2263) AS permit_geom
    FROM source
    WHERE upper(status) IN ('EXPIRED', 'CURRENT')
)

SELECT * FROM final
