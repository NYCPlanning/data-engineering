WITH mappluto_wi AS (

    SELECT * FROM {{ source('ceqr_survey_sources', 'dcp_mappluto_wi') }}

),

final AS (
    SELECT
        cast(bbl AS text) AS bbl,
        zonedist1,
        zonedist2,
        zonedist3,
        zonedist4,
        wkb_geometry
    FROM mappluto_wi
)

SELECT * FROM final
