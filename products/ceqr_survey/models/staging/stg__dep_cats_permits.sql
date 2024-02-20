-- stg__dep_cats_permits.sql

WITH source AS (
    SELECT * FROM {{ source('ceqr_survey_sources', 'dep_cats_permits') }}
),

final AS (
    SELECT
        'cats_permits' AS variable,
        applicationid,
        status,
        st_transform(geom::geometry, 2263) as geom
    FROM source
    WHERE UPPER(status) IN ('EXPIRED', 'CURRENT')
)

SELECT * FROM final
