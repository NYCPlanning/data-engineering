{{ config(materialized='table') }}

-- Every point where two straight topology edges properly cross (interiors intersect;
-- meeting at a shared node isn't a crossing), with a new node id for each point. A
-- crossing means two AtomicPolygons overlap slightly in the source - their shared
-- corner was digitized as two nodes further apart than the merge tolerance (e.g. APs
-- 4021400227 / 4021400220: 0.0023 ft, overlapping by 0.012 sq ft). Left unnoded,
-- ST_BuildArea can't polygonize the edges around it and the whole district builds
-- empty. int__topology__edges splits both edges at the point.
--
-- Arcs aren't checked; no arc crossing has been observed.

-- A crossing comes from two AtomicPolygons overlapping instead of sharing an edge, so
-- at least one edge of every crossing pair has a single owner. Probing from those
-- (~1k citywide) against all edges via the geom index keeps this cheap; probing every
-- edge against every neighbour is ~700k index lookups.
WITH one_owner AS (
    SELECT
        node_lo,
        node_hi,
        (array_agg(geom))[1] AS geom
    FROM {{ ref('int__topology__edges_unnoded') }}
    WHERE NOT is_arc
    GROUP BY node_lo, node_hi
    HAVING count(DISTINCT atomicid) = 1
),

candidate_pairs AS (
    SELECT
        a.node_lo AS a_lo,
        a.node_hi AS a_hi,
        b.node_lo AS b_lo,
        b.node_hi AS b_hi,
        a.geom AS a_geom,
        b.geom AS b_geom
    FROM one_owner AS a
    INNER JOIN {{ ref('int__topology__edges_unnoded') }} AS b
        ON a.geom && b.geom AND st_crosses(a.geom, b.geom)
    WHERE NOT b.is_arc
),

-- each crossing once, whichever side it was found from
pairs AS (
    SELECT DISTINCT ON (a_lo, a_hi, b_lo, b_hi)
        a_lo,
        a_hi,
        b_lo,
        b_hi,
        a_geom,
        b_geom
    FROM (
        SELECT
            a_lo,
            a_hi,
            b_lo,
            b_hi,
            a_geom,
            b_geom
        FROM candidate_pairs
        WHERE (a_lo, a_hi) < (b_lo, b_hi)
        UNION ALL
        SELECT
            b_lo,
            b_hi,
            a_lo,
            a_hi,
            b_geom,
            a_geom
        FROM candidate_pairs
        WHERE (b_lo, b_hi) < (a_lo, a_hi)
    ) AS ordered
),

points AS (
    SELECT
        p.a_lo,
        p.a_hi,
        p.b_lo,
        p.b_hi,
        d.geom
    FROM pairs AS p
    CROSS JOIN LATERAL st_dump(st_intersection(p.a_geom, p.b_geom)) AS d
    WHERE st_geometrytype(d.geom) = 'ST_Point'
),

numbered AS (
    SELECT
        *,
        (SELECT max(node_id) FROM {{ ref('int__topology__nodes') }})
        + dense_rank() OVER (ORDER BY st_x(geom), st_y(geom)) AS node_id
    FROM points
)

-- one row per (crossed edge, crossing point)
SELECT
    a_lo AS node_lo,
    a_hi AS node_hi,
    node_id,
    geom
FROM numbered
UNION ALL
SELECT
    b_lo AS node_lo,
    b_hi AS node_hi,
    node_id,
    geom
FROM numbered
