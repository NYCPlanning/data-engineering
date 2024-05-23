WITH panynj_airports_lga AS (
    SELECT * FROM {{ source('recipe_sources', 'panynj_lga_65db') }}
),

panynj_airports_jfk AS (
    SELECT * FROM {{ source('recipe_sources', 'panynj_jfk_65db') }}
),

all_airports AS (
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
)

SELECT
    variable_type AS flag_variable_type,
    variable_type,
    variable_id,
    raw_geom
FROM all_airports
