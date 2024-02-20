WITH panynj_airports_lga AS (

    SELECT * FROM {{ source('ceqr_survey_sources', 'panynj_lga_65db') }}

),
panynj_airports_jfk AS (

    SELECT * FROM {{ source('ceqr_survey_sources', 'panynj_jfk_65db') }}

)

SELECT
    wkb_geometry,
    'Laguardia Airport' AS "name"
FROM panynj_airports_lga
UNION
SELECT
    wkb_geometry,
    'John F. Kennedy Airport' AS "name"
FROM panynj_airports_jfk
