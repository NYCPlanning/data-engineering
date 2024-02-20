WITH arterial_highways_raw AS (

    SELECT * FROM {{ source('ceqr_survey_sources', 'dcm_arterial_highways') }}

)

SELECT
    name,
    wkb_geometry AS unbuffered_geom,
    ST_BUFFER(wkb_geometry, 75) AS buffered_geom
FROM arterial_highways_raw
WHERE "source" = 'Appendix H'
