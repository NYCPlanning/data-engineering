-- int__topology__edge_crossings. Same spelling as PG: DuckDB plans `a.geom && b.geom`
-- (and ST_Intersects / ST_Crosses alone) as a SPATIAL_JOIN, which builds an R-tree over
-- one side on the fly - no persistent index needed.
WITH one_owner AS (
    SELECT
        node_lo,
        node_hi,
        any_value(geom) AS geom
    FROM edges_unnoded
    WHERE NOT is_arc
    GROUP BY node_lo, node_hi
    HAVING count(DISTINCT atomicid) = 1
),

straight AS (
    SELECT node_lo, node_hi, geom FROM edges_unnoded WHERE NOT is_arc
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
    INNER JOIN straight AS b
        ON a.geom && b.geom AND st_crosses(a.geom, b.geom)
),

pairs AS (
    SELECT DISTINCT ON (a_lo, a_hi, b_lo, b_hi)
        a_lo, a_hi, b_lo, b_hi, a_geom, b_geom
    FROM (
        SELECT a_lo, a_hi, b_lo, b_hi, a_geom, b_geom
        FROM candidate_pairs
        WHERE (a_lo, a_hi) < (b_lo, b_hi)
        UNION ALL
        SELECT b_lo, b_hi, a_lo, a_hi, b_geom, a_geom
        FROM candidate_pairs
        WHERE (b_lo, b_hi) < (a_lo, a_hi)
    )
),

points AS (
    SELECT
        a_lo,
        a_hi,
        b_lo,
        b_hi,
        unnest(st_dump(st_intersection(a_geom, b_geom))).geom AS geom
    FROM pairs
),

numbered AS (
    SELECT
        *,
        (SELECT max(node_id) FROM nodes)
        + dense_rank() OVER (ORDER BY st_x(geom), st_y(geom)) AS node_id
    FROM points
    WHERE st_geometrytype(geom) = 'POINT'
)

SELECT a_lo AS node_lo, a_hi AS node_hi, node_id, st_x(geom) AS x, st_y(geom) AS y, geom
FROM numbered
UNION ALL
SELECT b_lo AS node_lo, b_hi AS node_hi, node_id, st_x(geom) AS x, st_y(geom) AS y, geom
FROM numbered
