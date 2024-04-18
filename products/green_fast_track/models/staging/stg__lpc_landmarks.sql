WITH lpc_landmarks AS (
    SELECT * FROM {{ source('recipe_sources', 'lpc_landmarks') }}
),

all_landmarks AS (
    SELECT
        lm_type,
        lm_name,
        status,
        last_actio,
        most_curre,
        -- For geodetic coordinates, X is longitude and Y is latitude
        ST_TRANSFORM(ST_SETSRID(ST_MAKEPOINT(longitude::float, latitude::float), 4326), 2263) AS raw_geom
    FROM lpc_landmarks
),

filtered_lpc_landmarks AS (
    SELECT
        'nyc_historic_buildings' AS variable_type,
        lm_name AS variable_id,
        raw_geom
    FROM all_landmarks
    WHERE
        (lm_type = 'Individual Landmark' OR lm_type = 'Interior Landmark')
        AND status = 'DESIGNATED'
        AND (last_actio = 'DESIGNATED' OR last_actio = 'DESIGNATED (AMENDMENT/MODIFICATION ACCEPTED)')
        AND most_curre = '1'
)

SELECT * FROM filtered_lpc_landmarks
