WITH panynj_airports_lga AS (
    SELECT * FROM {{ source('recipe_sources', 'panynj_lga_65db') }}
),

panynj_airports_jfk AS (
    SELECT * FROM {{ source('recipe_sources', 'panynj_jfk_65db') }}
)

SELECT
    'airport_noise_lga' AS variable_type,
    'LaGuardia Airport' AS variable_id,
    wkb_geometry AS raw_geom
FROM panynj_airports_lga
UNION
SELECT
    'airport_noise_jfk' AS variable_type,
    'John F. Kennedy Airport' AS variable_id,
    wkb_geometry AS raw_geom
FROM panynj_airports_jfk
