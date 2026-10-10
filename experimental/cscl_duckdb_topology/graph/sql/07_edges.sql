-- int__topology__edges: split every crossed straight edge at its crossing points.
WITH unnoded AS (
    SELECT * FROM edges_unnoded
),

crossed AS (
    SELECT
        e.*,
        row_number() OVER () AS edge_row
    FROM unnoded AS e
    WHERE
        NOT e.is_arc
        AND EXISTS (
            SELECT 1 FROM edge_crossings AS x
            WHERE x.node_lo = e.node_lo AND x.node_hi = e.node_hi
        )
),

edge_points AS (
    SELECT
        c.edge_row,
        0.0 AS frac,
        CASE WHEN st_equals(st_startpoint(c.geom), lo.geom) THEN c.node_lo ELSE c.node_hi END AS node_id,
        st_startpoint(c.geom) AS geom
    FROM crossed AS c
    INNER JOIN nodes AS lo ON c.node_lo = lo.node_id
    UNION ALL
    SELECT
        c.edge_row,
        1.0 AS frac,
        CASE WHEN st_equals(st_startpoint(c.geom), lo.geom) THEN c.node_hi ELSE c.node_lo END AS node_id,
        st_endpoint(c.geom) AS geom
    FROM crossed AS c
    INNER JOIN nodes AS lo ON c.node_lo = lo.node_id
    UNION ALL
    SELECT
        c.edge_row,
        st_linelocatepoint(c.geom, x.geom) AS frac,
        x.node_id,
        x.geom
    FROM crossed AS c
    INNER JOIN edge_crossings AS x
        ON c.node_lo = x.node_lo AND c.node_hi = x.node_hi
),

pieces AS (
    SELECT
        edge_row,
        node_id AS node_a,
        lead(node_id) OVER w AS node_b,
        st_makeline(geom, lead(geom) OVER w) AS geom
    FROM edge_points
    WINDOW w AS (PARTITION BY edge_row ORDER BY frac)
)

SELECT node_lo, node_hi, is_arc, geom, atomicid, water_flag
FROM unnoded AS e
WHERE
    NOT EXISTS (
        SELECT 1 FROM crossed AS c
        WHERE c.node_lo = e.node_lo AND c.node_hi = e.node_hi AND NOT e.is_arc
    )
UNION ALL
SELECT
    least(p.node_a, p.node_b) AS node_lo,
    greatest(p.node_a, p.node_b) AS node_hi,
    false AS is_arc,
    p.geom,
    c.atomicid,
    c.water_flag
FROM pieces AS p
INNER JOIN crossed AS c ON p.edge_row = c.edge_row
WHERE p.node_b IS NOT NULL AND p.node_a != p.node_b
