{{ config(
    materialized='table',
    indexes=[
        {'columns': ['node_lo', 'node_hi', 'is_arc']},
    ]
) }}

-- Which side of each topology edge each owner AtomicPolygon lies on: one row per
-- int__topology__edges row, with owner_left = the owner is on the left when walking the
-- edge from node_lo to node_hi.
--
-- Taken from ring orientation, not a probe point, so it holds however thin the AP is:
-- walking a ring, the AP's interior is on the left for a CCW shell or a CW hole. The
-- owner's own ring walks the edge in some direction (node_a -> node_b); the owner is
-- left of lo -> hi iff that direction agrees with lo -> hi. Orientation is the sign of
-- the shoelace sum over the raw ring vertices (arc mids included, so an arc counts as
-- its chord triangle - plenty for a sign).
--
-- Edges that are not one of their owner's own ring steps (orphan-split sub-edges and
-- crossing pieces, ~900 rows) take the opposite side of a co-owner that is; the few
-- left after that (~50) stay NULL, and consumers treat that owner's side as unknown.

WITH vertex_nodes AS (
    SELECT
        v.atomicid,
        v.part,
        v.ring,
        v.seq,
        v.is_arc_mid,
        ptn.node_id,
        st_x(v.geom) AS x,
        st_y(v.geom) AS y,
        lead(v.is_arc_mid) OVER w AS next_is_mid,
        lead(st_x(v.geom)) OVER w AS next_x,
        lead(st_y(v.geom)) OVER w AS next_y
    FROM {{ ref('int__topology__vertices') }} AS v
    LEFT JOIN {{ ref('int__topology__exact_points') }} AS ec
        -- arc mids never match: NULL key (a left-only predicate in the ON clause makes
        -- DuckDB fall back to a nested-loop join)
        ON (CASE WHEN v.is_arc_mid THEN NULL ELSE v.geom END) = ec.geom
    LEFT JOIN {{ ref('int__topology__point_to_node') }} AS ptn ON ec.point_id = ptn.exact_point_id
    WINDOW w AS (PARTITION BY v.atomicid, v.part, v.ring ORDER BY v.seq)
),

ring_orientation AS (
    SELECT
        atomicid,
        part,
        ring,
        sum(x * next_y - next_x * y) > 0 AS is_ccw
    FROM vertex_nodes
    WHERE next_x IS NOT NULL
    GROUP BY atomicid, part, ring
),

ring_steps AS (
    SELECT
        atomicid,
        part,
        ring,
        node_id AS node_a,
        lead(node_id) OVER w AS node_b,
        coalesce(next_is_mid, FALSE) AS is_arc
    FROM vertex_nodes
    WHERE NOT is_arc_mid
    WINDOW w AS (PARTITION BY atomicid, part, ring ORDER BY seq)
),

-- interior on the left of this step's own direction, and that direction is lo -> hi
step_flags AS (
    SELECT
        s.atomicid,
        least(s.node_a, s.node_b) AS node_lo,
        greatest(s.node_a, s.node_b) AS node_hi,
        s.is_arc,
        (o.is_ccw = (s.ring = 1)) = (s.node_a < s.node_b) AS left_of_lo_hi
    FROM ring_steps AS s
    INNER JOIN ring_orientation AS o
        ON s.atomicid = o.atomicid AND s.part = o.part AND s.ring = o.ring
    WHERE s.node_b IS NOT NULL AND s.node_a != s.node_b
),

-- an owner can walk the same edge more than once (a ring touching itself); a
-- contradiction is left NULL rather than picking a side
step_sides AS (
    SELECT
        atomicid,
        node_lo,
        node_hi,
        is_arc,
        CASE
            WHEN bool_and(left_of_lo_hi) THEN TRUE
            WHEN NOT bool_or(left_of_lo_hi) THEN FALSE
        END AS owner_left
    FROM step_flags
    GROUP BY atomicid, node_lo, node_hi, is_arc
),

direct AS (
    SELECT
        e.node_lo,
        e.node_hi,
        e.is_arc,
        e.atomicid,
        ss.owner_left
    FROM {{ ref('int__topology__edges') }} AS e
    LEFT JOIN step_sides AS ss
        ON
            e.atomicid = ss.atomicid
            AND e.node_lo = ss.node_lo
            AND e.node_hi = ss.node_hi
            AND e.is_arc = ss.is_arc
),

-- per edge, the side its resolved owners agree on
resolved AS (
    SELECT
        node_lo,
        node_hi,
        is_arc,
        CASE
            WHEN bool_and(owner_left) THEN TRUE
            WHEN NOT bool_or(owner_left) THEN FALSE
        END AS owner_left
    FROM direct
    WHERE owner_left IS NOT NULL
    GROUP BY node_lo, node_hi, is_arc
)

SELECT
    d.node_lo,
    d.node_hi,
    d.is_arc,
    d.atomicid,
    coalesce(d.owner_left, NOT r.owner_left) AS owner_left,
    CASE
        WHEN d.owner_left IS NOT NULL THEN 'ring'
        WHEN r.owner_left IS NOT NULL THEN 'co_owner'
    END AS side_source
FROM direct AS d
-- (resolved has one row per edge key and d.owner_left wins above, so no
-- "d.owner_left IS NULL" here - a left-only ON predicate makes DuckDB nested-loop)
LEFT JOIN resolved AS r
    ON
        d.node_lo = r.node_lo
        AND d.node_hi = r.node_hi
        AND d.is_arc = r.is_arc
