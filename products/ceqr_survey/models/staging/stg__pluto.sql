WITH mappluto_wi AS (

    SELECT * FROM {{ source('ceqr_survey_sources', 'dcp_mappluto_wi') }}

),

final AS (
    SELECT
        CAST(bbl AS text) AS bbl,
        zonedist1,
        zonedist2,
        zonedist3,
        zonedist4,
        spdist1,
        spdist2,
        spdist3,
        ST_TRANSFORM(wkb_geometry, 2263) AS geom
    FROM mappluto_wi
)

SELECT * FROM final
