{{ config(
    materialized='table',
    indexes=[
        {'columns': ['node_lo', 'node_hi', 'is_arc']},
        {'columns': ['atomicid']},
        {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

-- Before noding (int__topology__edges splits edges that cross - see that model).
--
-- Every ring edge of every in-scope AtomicPolygon: one row per (edge, owning atomicid),
-- keyed by (node_lo, node_hi, is_arc) so the same physical edge traced by two adjacent
-- AtomicPolygons in opposite winding order lands on the same key. District-agnostic -
-- the district boundary layer (macros/district_boundary.sql) joins
-- int__topology__ap_entities on atomicid and classifies edges by who owns them.
--
-- Arcs: an arc is ONE edge between its two endpoint nodes; its mid control point is not
-- a node (two AtomicPolygons tracing the same arc can pick different mids). is_arc is
-- part of the key, so a straight edge and an arc between the same two nodes stay
-- distinct with no tolerance involved; qa__topology__arc_disagreements flags the
-- (so far never observed) case of two different arcs sharing both endpoints. Each arc
-- key gets one canonical curve, shared bit-for-bit by all its owners. A full circle
-- (start == end) would collapse here; none exist in the source.
--
-- Orphan edge splitting: a straight edge with only one owner is often its neighbour's
-- edge digitized with fewer vertices (the neighbour has extra, collinear intermediate
-- points). For each such orphan A-B, look for an alternate path of up to
-- ap_topology_edge_split_max_hops existing edges from A to B whose total length equals
-- the direct A-B distance within ap_topology_edge_split_tolerance_ft. By the triangle
-- inequality that's only possible if every intermediate node lies on the A-B line, so
-- this one scalar test is an exact collinearity check for any number of hops. A match
-- replaces the orphan's edge with the neighbour's sub-edges (same atomicid, existing
-- node keys - no inferred ownership), which is degree-neutral by construction. An orphan
-- with no such match (two AtomicPolygons genuinely disagreeing about a corner) stays an
-- orphan and is handled downstream (it reads as a boundary edge; any thin loop it
-- creates is stripped as noise).

WITH RECURSIVE pts_with_nodes AS (
    SELECT
        p.atomicid,
        p.water_flag,
        p.part,
        p.ring,
        p.seq,
        p.is_arc_mid,
        p.geom AS raw_geom,
        n.node_id,
        n.geom AS node_geom
    FROM {{ ref('int__topology__vertices') }} AS p
    LEFT JOIN {{ ref('int__topology__exact_points') }} AS ec
        ON p.geom = ec.geom AND NOT p.is_arc_mid
    LEFT JOIN {{ ref('int__topology__point_to_node') }} AS ptn ON ec.point_id = ptn.exact_point_id
    LEFT JOIN {{ ref('int__topology__nodes') }} AS n ON ptn.node_id = n.node_id
),

-- For each node point, whether the very next raw point is an arc mid (and its coords).
with_next AS (
    SELECT
        *,
        lead(is_arc_mid) OVER w AS next_is_mid,
        lead(raw_geom) OVER w AS next_raw_geom
    FROM pts_with_nodes
    WINDOW w AS (PARTITION BY atomicid, part, ring ORDER BY seq)
),

raw_edges AS (
    SELECT
        atomicid,
        water_flag,
        node_id AS node_a,
        lead(node_id) OVER w AS node_b,
        node_geom AS geom_a,
        lead(node_geom) OVER w AS geom_b,
        coalesce(next_is_mid, FALSE) AS is_arc,
        CASE WHEN next_is_mid THEN next_raw_geom END AS arc_mid
    FROM with_next
    WHERE NOT is_arc_mid
    WINDOW w AS (PARTITION BY atomicid, part, ring ORDER BY seq)
),

-- One canonical curve per arc key: the lowest-atomicid owner's mid control point,
-- through the canonical endpoint node coordinates, oriented lower -> higher node id,
-- linearized with the same parameters as public.linearize() (ST_CurveToLine, 0.00025 ft
-- max deviation) - i.e. the same way prod's layers are - with both ends snapped exactly
-- onto their node coordinates so ST_BuildArea sees them join. Every owner gets this
-- same geometry, bit for bit, regardless of winding or its own choice of mid.
arc_geoms AS (
    SELECT
        node_lo,
        node_hi,
        st_setpoint(st_setpoint(line, 0, lo_geom), -1, hi_geom) AS geom
    FROM (
        SELECT DISTINCT ON (node_lo, node_hi)
            least(node_a, node_b) AS node_lo,
            greatest(node_a, node_b) AS node_hi,
            CASE WHEN node_a < node_b THEN geom_a ELSE geom_b END AS lo_geom,
            CASE WHEN node_a < node_b THEN geom_b ELSE geom_a END AS hi_geom,
            st_curvetoline(
                st_setsrid(st_geomfromtext(format(
                    'CIRCULARSTRING(%s %s, %s %s, %s %s)',
                    st_x(CASE WHEN node_a < node_b THEN geom_a ELSE geom_b END),
                    st_y(CASE WHEN node_a < node_b THEN geom_a ELSE geom_b END),
                    st_x(arc_mid), st_y(arc_mid),
                    st_x(CASE WHEN node_a < node_b THEN geom_b ELSE geom_a END),
                    st_y(CASE WHEN node_a < node_b THEN geom_b ELSE geom_a END)
                )), st_srid(arc_mid)),
                0.00025, 1
            ) AS line
        FROM raw_edges
        WHERE is_arc AND node_b IS NOT NULL AND node_a != node_b
        ORDER BY least(node_a, node_b), greatest(node_a, node_b), atomicid
    ) AS canonical
),

edges AS (
    SELECT
        r.atomicid,
        r.water_flag,
        r.node_a,
        r.node_b,
        least(r.node_a, r.node_b) AS node_lo,
        greatest(r.node_a, r.node_b) AS node_hi,
        r.is_arc,
        -- chord: what the orphan path search below measures (its triangle-inequality
        -- test is about straight-line distance between the two nodes)
        st_distance(r.geom_a, r.geom_b) AS chord_length,
        coalesce(ag.geom, st_makeline(r.geom_a, r.geom_b)) AS geom
    FROM raw_edges AS r
    LEFT JOIN arc_geoms AS ag
        ON r.is_arc AND least(r.node_a, r.node_b) = ag.node_lo AND greatest(r.node_a, r.node_b) = ag.node_hi
    WHERE r.node_b IS NOT NULL AND r.node_a != r.node_b
),

edge_owner_totals AS (
    SELECT
        node_lo,
        node_hi,
        is_arc,
        count(DISTINCT atomicid) AS total_owners
    FROM edges
    GROUP BY node_lo, node_hi, is_arc
),

orphan_edges AS (
    SELECT
        e.atomicid,
        e.water_flag,
        e.node_lo,
        e.node_hi,
        e.geom,
        e.chord_length AS direct_length
    FROM edges AS e
    INNER JOIN edge_owner_totals AS t
        ON e.node_lo = t.node_lo AND e.node_hi = t.node_hi AND e.is_arc = t.is_arc
    -- straight edges only: the path-length match is a collinearity test, meaningless
    -- for a curve
    WHERE t.total_owners = 1 AND NOT e.is_arc
),

-- Undirected `edges` as a directed adjacency list (both directions), so path-walking
-- below is a plain join.
directed_edges AS (
    SELECT
        node_lo AS from_node,
        node_hi AS to_node,
        geom,
        chord_length AS len
    FROM edges
    WHERE NOT is_arc
    UNION ALL
    SELECT
        node_hi AS from_node,
        node_lo AS to_node,
        geom,
        chord_length AS len
    FROM edges
    WHERE NOT is_arc
),

-- Bounded path search: for each orphan A-B, walk other edges outward from A (never
-- revisiting a node, never taking the direct A-B edge as the very first hop - that
-- would just be re-walking the edge being tested) and stop extending any branch once
-- its cumulative length already exceeds the direct distance plus tolerance, since
-- length only accumulates hop over hop. This keeps the search tight regardless of the
-- hop cap: real estate boundaries don't have chains of near-zero-length edges, so a
-- detour that's genuinely the "same line, more vertices" burns through its length
-- budget in a handful of hops or not at all.
path_search (
    a, b, atomicid, water_flag, direct_length, cur_node, path_nodes, path_geoms, path_length, hops
) AS (
    SELECT
        node_lo,
        node_hi,
        atomicid,
        water_flag,
        direct_length,
        node_lo AS cur_node,
        ARRAY[node_lo] AS path_nodes,
        ARRAY[]::geometry[] AS path_geoms,
        0.0::double precision AS path_length,
        0 AS hops
    FROM orphan_edges

    UNION ALL

    SELECT
        ps.a,
        ps.b,
        ps.atomicid,
        ps.water_flag,
        ps.direct_length,
        de.to_node,
        ps.path_nodes || de.to_node,
        ps.path_geoms || de.geom,
        ps.path_length + de.len,
        ps.hops + 1
    FROM path_search AS ps
    INNER JOIN directed_edges AS de ON ps.cur_node = de.from_node
    WHERE
        ps.hops < {{ var('ap_topology_edge_split_max_hops') }}
        AND NOT (de.to_node = any(ps.path_nodes))
        AND NOT (ps.hops = 0 AND de.to_node = ps.b)
        AND ps.path_length + de.len <= ps.direct_length + {{ var('ap_topology_edge_split_tolerance_ft') }}
),

-- A completed detour (back at B, at least 2 hops so it's genuinely an alternate route,
-- not the direct edge itself) whose total length matches the direct distance - by the
-- triangle inequality, only possible if every intermediate node lies (essentially)
-- exactly on the straight line between A and B.
split_matches AS (
    SELECT DISTINCT ON (a, b)
        a,
        b,
        atomicid,
        water_flag,
        path_nodes,
        path_geoms,
        abs(path_length - direct_length) AS length_diff
    FROM path_search
    WHERE
        cur_node = b
        AND hops >= 2
        AND abs(path_length - direct_length) <= {{ var('ap_topology_edge_split_tolerance_ft') }}
    ORDER BY a, b, abs(path_length - direct_length)
),

split_sub_edges AS (
    SELECT
        sm.atomicid,
        sm.water_flag,
        least(sm.path_nodes[i], sm.path_nodes[i + 1]) AS node_lo,
        greatest(sm.path_nodes[i], sm.path_nodes[i + 1]) AS node_hi,
        FALSE AS is_arc,
        sm.path_geoms[i] AS geom
    FROM split_matches AS sm
    CROSS JOIN LATERAL generate_series(1, array_length(sm.path_nodes, 1) - 1) AS i
),

-- The matched orphan's own direct edge is replaced by its split sub-edges (same
-- atomicid) - everything else passes through unchanged.
edges_densified AS (
    SELECT
        e.atomicid,
        e.water_flag,
        e.node_lo,
        e.node_hi,
        e.is_arc,
        e.geom
    FROM edges AS e
    LEFT JOIN split_matches AS sm
        ON
            e.atomicid = sm.atomicid AND NOT e.is_arc
            AND e.node_lo = least(sm.a, sm.b) AND e.node_hi = greatest(sm.a, sm.b)
    WHERE sm.a IS NULL
    UNION ALL
    SELECT
        atomicid,
        water_flag,
        node_lo,
        node_hi,
        is_arc,
        geom
    FROM split_sub_edges
)

SELECT
    node_lo,
    node_hi,
    is_arc,
    geom,
    atomicid,
    water_flag
FROM edges_densified
