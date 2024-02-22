WITH panynj_airports_lga AS (
    SELECT * FROM {{ source('ceqr_survey_sources', 'panynj_lga_65db') }}
),

panynj_airports_jfk AS (
    SELECT * FROM {{ source('ceqr_survey_sources', 'panynj_jfk_65db') }}
)

SELECT
    wkb_geometry AS buffer,
    wkb_geometry AS raw_geom,
    'Laguardia Airport' AS variable_id,
    'airport' AS variable_type
FROM panynj_airports_lga
UNION
SELECT
    wkb_geometry AS buffer,
    wkb_geometry AS raw_geom,
    'John F. Kennedy Airport' AS variable_id,
    'airport' AS variable_type
FROM panynj_airports_jfk
