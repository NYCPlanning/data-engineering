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
        raw_geom AS point_geom,
        pluto.bbl,
        pluto.geom AS lot_geom
    FROM lpc_landmarks
    -- If, for example, there's a lamppost in a park, we don't want to use the polygon for the entire park.
    LEFT JOIN
        pluto
        ON ST_CONTAINS(pluto.geom, lpc_landmarks.raw_geom) AND variable_id != 'Historic Street Lampposts'
),

-- There are a few different cases for deduping:
--  A. landmarks that share a name, e.g. "Historic Lampposts" that should't match to a PLUTO bbl,
--      so should be combined into one geometry, because they lack a unique name for a variable_id
--  B. landmarks that share a name and all are not contained in any lots
--      so should use the point(s) to buffer
--  C. landmarks that share a name and all are contained in at least one lot
--      so should use the lot(s) to buffer
--  D. landmarks that share a name and some are contained in at least one lot
--      so should use only the lot(s) to buffer, not both the point(s) and lot(s)
grouped_landmarks AS (
    SELECT
        variable_type,
        variable_id,
        STRING_AGG(DISTINCT bbl, ', ') AS bbls,
        ST_MULTI(ST_UNION(point_geom)) AS point_geom,
        ST_MULTI(ST_UNION(lot_geom)) AS lot_geom
    FROM landmarks_with_pluto
    GROUP BY variable_type, variable_id
),

resolved_landmarks AS (
    SELECT
        variable_type,
        variable_id,
        bbls,
        point_geom,
        lot_geom,
        COALESCE(lot_geom, point_geom) AS raw_geom
    FROM grouped_landmarks
),

buffered_landmarks AS (
    SELECT
        variable_type,
        variable_id,
        bbls,
        point_geom,
        lot_geom,
        raw_geom,
        ST_BUFFER(raw_geom, 90) AS buffer
    FROM resolved_landmarks
)

SELECT * FROM buffered_landmarks
