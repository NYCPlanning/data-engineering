WITH arterial_highways_raw AS (
    SELECT * FROM {{ source('ceqr_survey_sources', 'dcm_arterial_highways') }}
),

filtered AS (
    SELECT
        name,
        ST_UNION(wkb_geometry) AS wkb_geometry
    FROM arterial_highways_raw
    WHERE source = 'Appendix H'
    GROUP BY name
)

SELECT
    name AS variable_id,
    'arterial_highway' AS variable_type,
    wkb_geometry AS raw_geom,
    ST_BUFFER(wkb_geometry, 75) AS buffer
FROM filtered
