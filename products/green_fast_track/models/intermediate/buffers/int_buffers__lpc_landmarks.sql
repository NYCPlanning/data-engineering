WITH pluto AS (
    SELECT
        bbl,
        landuse,
        zonedist1,
        geom
    FROM {{ ref('stg__pluto') }}
),

lpc_landmarks AS (
    SELECT
        variable_type,
        variable_id,
        raw_geom
    FROM {{ ref('stg__lpc_landmarks') }}
),

landmarks_with_pluto AS (
    SELECT
        variable_type,
        variable_id,
        pluto.bbl,
        pluto.geom,
        COALESCE(pluto.geom, lpc_landmarks.raw_geom) AS coalesced_geom
    FROM lpc_landmarks
    -- If, for example, there's a lamppost in a park, we don't want to use the polygon for the entire park.
    LEFT JOIN
        pluto
        ON ST_CONTAINS(pluto.geom, lpc_landmarks.raw_geom) AND variable_id != 'Historic Street Lampposts'
),

-- There are a few different cases for deduping.
-- There are landmarks that share a name, e.g. "Historic Lampposts" that won't match to a PLUTO bbl,
-- and should be combined into one geometry, because they lack a unique name for a variable_id.
-- Otherwise we should group by the PLUTO bbl, joining together the names of the individual landmarks
-- when there are multiple per BBL. e.g. The Brooklyn Navy Yard has two buildings within that are both
-- landmarks.
grouped_landmarks AS (
    SELECT
        variable_type,
        variable_id,
        STRING_AGG(DISTINCT bbl, ', ') AS bbls,
        ST_UNION(coalesced_geom) AS geom
    FROM landmarks_with_pluto
    GROUP BY variable_type, variable_id
),

buffered_landmarks AS (
    SELECT
        variable_type,
        variable_id,
        bbls,
        geom AS raw_geom,
        ST_BUFFER(geom, 90) AS buffer
    FROM grouped_landmarks
)

SELECT * FROM buffered_landmarks
