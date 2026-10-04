{{ config(materialized='table') }}

-- Checks int__topology__edges' arc keying. An arc is keyed only on its endpoints (+
-- is_arc) and all owners share one canonical curve - right when owners trace the same
-- arc with different mid control points, wrong if two different arcs share both
-- endpoints. Per (arc key, owner): distance from the owner's own mid point to the
-- canonical curve. ~0 (quantization + linearization tolerance, < 0.001 ft) means same
-- arc; anything material means two curves collided on one key.

WITH pt_nodes AS (
    SELECT
        p.atomicid,
        p.part,
        p.ring,
        p.seq,
        p.is_arc_mid,
        p.geom,
        ptn.node_id
    FROM {{ ref('int__topology__vertices') }} AS p
    LEFT JOIN {{ ref('int__topology__exact_points') }} AS ec
        ON p.geom = ec.geom AND NOT p.is_arc_mid
    LEFT JOIN {{ ref('int__topology__point_to_node') }} AS ptn ON ec.point_id = ptn.exact_point_id
),

mids AS (
    SELECT
        atomicid,
        geom AS mid_geom,
        lag(node_id) OVER w AS node_a,
        lead(node_id) OVER w AS node_b,
        is_arc_mid
    FROM pt_nodes
    WINDOW w AS (PARTITION BY atomicid, part, ring ORDER BY seq)
),

canonical AS (
    SELECT DISTINCT ON (node_lo, node_hi)
        node_lo,
        node_hi,
        geom
    FROM {{ ref('int__topology__edges') }}
    WHERE is_arc
    ORDER BY node_lo, node_hi, atomicid
),

owners AS (
    SELECT
        node_lo,
        node_hi,
        count(DISTINCT atomicid) AS n_owners
    FROM {{ ref('int__topology__edges') }}
    WHERE is_arc
    GROUP BY node_lo, node_hi
)

SELECT
    c.node_lo,
    c.node_hi,
    o.n_owners,
    m.atomicid,
    round(st_distance(m.mid_geom, c.geom)::numeric, 6) AS mid_offset_ft,
    round(st_length(c.geom)::numeric, 3) AS arc_length_ft
FROM mids AS m
INNER JOIN canonical AS c
    ON least(m.node_a, m.node_b) = c.node_lo AND greatest(m.node_a, m.node_b) = c.node_hi
INNER JOIN owners AS o ON c.node_lo = o.node_lo AND c.node_hi = o.node_hi
WHERE m.is_arc_mid
