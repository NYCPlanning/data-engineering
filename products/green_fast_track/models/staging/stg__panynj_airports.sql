WITH panynj_airports_lga AS (
    SELECT * FROM {{ source('recipe_sources', 'panynj_lga_65db') }}
),

panynj_airports_jfk AS (
    SELECT * FROM {{ source('recipe_sources', 'panynj_jfk_65db') }}
)

SELECT
    wkb_geometry AS buffer,
    wkb_geometry AS raw_geom,
    'LaGuardia Airport' AS variable_id,
    'airport_noise_lga' AS variable_type
FROM panynj_airports_lga
UNION
SELECT
    wkb_geometry AS buffer,
    wkb_geometry AS raw_geom,
    'John F. Kennedy Airport' AS variable_id,
    'airport_noise_jfk' AS variable_type
FROM panynj_airports_jfk
