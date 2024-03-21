WITH lpc_landmarks AS (
    SELECT * FROM {{ source('recipe_sources', 'lpc_landmarks') }}
)

SELECT
    bbl,
    lm_type,
    lm_name,
    status,
    last_actio,
    most_curre,
    latitude,
    longitude,
    -- For geodetic coordinates, X is longitude and Y is latitude
    ST_TRANSFORM(ST_SETSRID(ST_MAKEPOINT(longitude::float, latitude::float), 4326), 2263) AS raw_geom
FROM lpc_landmarks
