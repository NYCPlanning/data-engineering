WITH pluto AS (
    SELECT
        bbl,
        landuse,
        zonedist1,
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

landmarks_with_pluto AS (
    SELECT
        lm_name,
        pluto.bbl,
        pluto.geom,
        COALESCE(pluto.geom, filtered_lpc_landmarks.raw_geom) AS coalesced_geom
    FROM filtered_lpc_landmarks
    -- If, for example, there's a lamppost in a park, we don't want to use the polygon for the entire park.
    LEFT JOIN
        pluto
        ON ST_CONTAINS(pluto.geom, filtered_lpc_landmarks.raw_geom) AND lm_name != 'Historic Street Lampposts'
),

-- There are a few different cases for deduping.
-- There are landmarks that share a name, e.g. "Historic Lampposts" that won't match to a PLUTO bbl,
-- and should be combined into one geometry, because they lack a unique name for a variable_id.
-- Otherwise we should group by the PLUTO bbl, joining together the names of the individual landmarks
-- when there are multiple per BBL. e.g. The Brooklyn Navy Yard has two buildings within that are both
-- landmarks.
grouped_landmarks AS (
    SELECT
        lm_name,
        STRING_AGG(DISTINCT bbl, ', ') AS bbls,
        ST_UNION(coalesced_geom) AS geom
    FROM landmarks_with_pluto
    GROUP BY lm_name
),

buffered_landmarks AS (
    SELECT
        'nyc_historic_buildings' AS variable_type,
        lm_name AS variable_id,
        bbls,
        geom AS raw_geom,
        ST_BUFFER(geom, 90) AS buffer
    FROM grouped_landmarks
)

SELECT * FROM buffered_landmarks
