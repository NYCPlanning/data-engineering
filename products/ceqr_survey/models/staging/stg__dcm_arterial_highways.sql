WITH arterial_highways_raw AS (
    SELECT * FROM {{ source('ceqr_survey_sources', 'dcm_arterial_highways') }}
)

SELECT
    name AS variable_id,
    'arterial_highway' AS variable_type,
    wkb_geometry AS raw_geom,
    ST_BUFFER(wkb_geometry, 75) AS buffer
FROM arterial_highways_raw
WHERE source = 'Appendix H'
