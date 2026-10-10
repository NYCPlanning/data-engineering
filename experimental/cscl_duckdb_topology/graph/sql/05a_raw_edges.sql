-- First half of int__topology__edges_unnoded (pts_with_nodes -> with_next -> raw_edges),
-- materialized so the arc check (arc_reconstruct.sql) can see each arc's mid point.
WITH pts_with_nodes AS (
    SELECT
        p.atomicid,
        p.water_flag,
        p.part,
        p.ring,
        p.seq,
        p.is_arc_mid,
        p.x AS raw_x,
        p.y AS raw_y,
        n.node_id,
        n.x AS node_x,
        n.y AS node_y,
        n.geom AS node_geom
    FROM vertices AS p
    -- PG: ON p.geom = ec.geom AND NOT p.is_arc_mid. DuckDB turns a one-sided predicate
    -- in a LEFT JOIN's ON clause into a blockwise nested-loop join; folding it into the
    -- join key keeps it a hash join (a NULL key never matches).
    LEFT JOIN exact_points AS ec
        ON (CASE WHEN NOT p.is_arc_mid THEN p.x END) = ec.x AND p.y = ec.y
    LEFT JOIN point_to_node AS ptn ON ec.point_id = ptn.exact_point_id
    LEFT JOIN nodes AS n ON ptn.node_id = n.node_id
),

with_next AS (
    SELECT
        *,
        lead(is_arc_mid) OVER w AS next_is_mid,
        lead(raw_x) OVER w AS next_raw_x,
        lead(raw_y) OVER w AS next_raw_y
    FROM pts_with_nodes
    WINDOW w AS (PARTITION BY atomicid, part, ring ORDER BY seq)
)

SELECT
    atomicid,
    water_flag,
    node_id AS node_a,
    lead(node_id) OVER w AS node_b,
    node_x AS a_x,
    node_y AS a_y,
    lead(node_x) OVER w AS b_x,
    lead(node_y) OVER w AS b_y,
    node_geom AS geom_a,
    lead(node_geom) OVER w AS geom_b,
    coalesce(next_is_mid, false) AS is_arc,
    CASE WHEN next_is_mid THEN next_raw_x END AS mid_x,
    CASE WHEN next_is_mid THEN next_raw_y END AS mid_y
FROM with_next
WHERE NOT is_arc_mid
WINDOW w AS (PARTITION BY atomicid, part, ring ORDER BY seq)
