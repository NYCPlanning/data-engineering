WITH pluto AS (
    SELECT
        bbl,
        landuse,
        geom
    FROM {{ ref('stg__pluto') }}
),

filtered_lpc_landmarks AS (
    SELECT * FROM {{ ref('stg__lpc_landmarks') }}
    WHERE
        (lm_type = 'Individual Landmark' OR lm_type = 'Interior Landmark')
        AND status = 'DESIGNATED'
        AND (last_actio = 'DESIGNATED' OR last_actio = 'DESIGNATED (AMENDMENT/MODIFICATION ACCEPTED)')
        AND most_curre = '1'
),

landmarks_with_maybe_pluto_geoms AS (
    SELECT
        lm_name,
        'lpc_landmark' AS variable_type,
        COALESCE(pluto.bbl, lm.lm_name) AS variable_id,
        COALESCE(pluto.geom, lm.raw_geom) AS raw_geom,
        ST_BUFFER(COALESCE(pluto.geom, lm.raw_geom), 90) AS buffer
    FROM filtered_lpc_landmarks AS lm
    LEFT JOIN pluto ON (lm.bbl = pluto.bbl OR ST_CONTAINS(pluto.geom, lm.raw_geom))
)

SELECT * FROM landmarks_with_maybe_pluto_geoms
