WITH vents_raw AS (

    SELECT * FROM {{ source('ceqr_survey_sources', 'dcp_air_quality_vent_towers') }}

)

SELECT
    bbl,
    name,
    wkb_geometry AS unbuffered_shape,
    ST_BUFFER(wkb_geometry, 75) AS buffered_shape
FROM vents_raw;
