WITH lpc_landmarks AS (
    -- ~0.4% of records (mostly historic districts, interior landmarks, and multi-site
    -- designations) have no geometry in the source archive -- see the
    -- lpc_landmarks_wkb_geometry_mostly_not_null test on models/_sources.yml
    SELECT * FROM {{ source('recipe_sources', 'lpc_landmarks') }}
    WHERE wkb_geometry IS NOT NULL
),

all_landmarks AS (
    SELECT
        lm_type,
        lm_name,
        status,
        last_actio,
        most_curre,
        ST_TRANSFORM(wkb_geometry, 2263) AS raw_geom
    FROM lpc_landmarks
),

filtered_lpc_landmarks AS (
    SELECT
        'nyc_historic_buildings' AS variable_type,
        lm_name AS variable_id,
        raw_geom
    FROM all_landmarks
    WHERE
        status = 'DESIGNATED'
        AND (last_actio = 'DESIGNATED' OR last_actio = 'DESIGNATED (AMENDMENT/MODIFICATION ACCEPTED)')
        AND most_curre = '1'
)

SELECT * FROM filtered_lpc_landmarks
