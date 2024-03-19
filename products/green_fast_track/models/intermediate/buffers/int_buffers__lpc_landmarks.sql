WITH pluto AS (
    SELECT
        bbl,
        landuse,
        zonedist1,
        geom
    FROM {{ ref('stg__pluto') }}
),


filtered_lpc_landmarks AS (
    SELECT
        lm_name,
        pluto.bbl,
        pluto.geom,
        COALESCE(pluto.geom, lm.raw_geom) AS coalesced_geom
    FROM {{ ref('stg__lpc_landmarks') }} AS lm
    -- If, for example, there's a lamppost in a park, we don't want to use the polygon for the entire park.
    LEFT JOIN pluto ON ST_CONTAINS(pluto.geom, lm.raw_geom) AND pluto.zonedist1 != 'PARK'
    WHERE
        (lm_type = 'Individual Landmark' OR lm_type = 'Interior Landmark')
        AND status = 'DESIGNATED'
        AND (last_actio = 'DESIGNATED' OR last_actio = 'DESIGNATED (AMENDMENT/MODIFICATION ACCEPTED)')
        AND most_curre = '1'
),

-- There are a few different cases for deduping.
-- There are landmarks that share a name, e.g. "Historic Lampposts" that won't match to a PLUTO bbl,
-- and should be combined into one geometry, because they lack a unique name for a variable_id.
-- Otherwise we should group by the PLUTO bbl, joining together the names of the individual landmarks
-- when there are multiple per BBL. e.g. The Brooklyn Navy Yard has two buildings within that are both
-- landmarks.
deduped_lpc_landmarks AS (
    SELECT
        STRING_AGG(DISTINCT lm_name, ', ') AS lm_names,
        ST_UNION(coalesced_geom) AS geom
    FROM filtered_lpc_landmarks
    GROUP BY bbl, COALESCE(bbl, lm_name)
)


SELECT
    lm_names AS variable_id,
    'historic_landmark' AS variable_type,
    geom AS raw_geom,
    ST_BUFFER(geom, 90) AS buffer
FROM deduped_lpc_landmarks
