{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['globalid']},
    ]
) }}

-- The AP topology edges each segment runs along. One row per step - a pair of
-- consecutive segment vertices that sit on topology nodes (int__segment_topology_vertices)
-- - and, for a step matched to a path of several edges, one row per edge. Read by
-- int__segment_topology_sides.
--
-- Matching, cheapest first:
--   edge      - the edge between the step's two nodes, if their lengths agree within
--               lion_lr_edge_match_length_tolerance_ft: a hash join on node ids.
--   path      - otherwise a path of up to lion_lr_edge_match_max_hops edges between the
--               two nodes, every node of it within lion_lr_edge_match_max_offset_ft of
--               the segment, with the same total length: the AP edge has a vertex the
--               centerline doesn't (often a few thousandths of a foot off a straight
--               line), or a crossing split it. Only ~2k steps get this far.
--   same_node - both vertices snapped to one node; covered if no longer than the
--               length tolerance.
--   none      - no match.
-- Anything with geometry between the step's nodes (an arc, segment vertices that didn't
-- snap, a path) must also lie within lion_lr_edge_match_max_offset_ft of the segment
-- (Hausdorff), so "same length" can't pair two different lines. Arcs need no special
-- case: their interior points never snap on either side, so an arc is matched on its
-- endpoints, its length and this offset.

{% set len_tol = var('lion_lr_edge_match_length_tolerance_ft') %}
{% set max_offset = var('lion_lr_edge_match_max_offset_ft') %}
{% set max_hops = var('lion_lr_edge_match_max_hops') %}

WITH RECURSIVE segments AS (
    SELECT
        globalid,
        geom,
        st_length(geom) AS seg_len
    FROM {{ ref('int__segments') }}
),

-- consecutive snapped vertices: an index lookup per segment, so the window only ever
-- sorts one segment's vertices
steps AS (
    SELECT
        s.globalid,
        s.seg_len,
        vs.idx_a,
        vs.idx_b,
        vs.node_a,
        vs.node_b,
        vs.along_a,
        vs.along_b,
        vs.along_b - vs.along_a AS step_len
    FROM segments AS s
    CROSS JOIN
        LATERAL (
            SELECT
                v.idx AS idx_a,
                lead(v.idx) OVER w AS idx_b,
                v.node_id AS node_a,
                lead(v.node_id) OVER w AS node_b,
                v.along_ft AS along_a,
                lead(v.along_ft) OVER w AS along_b
            FROM {{ ref('int__segment_topology_vertices') }} AS v
            WHERE v.globalid = s.globalid
            WINDOW w AS (ORDER BY v.idx)
        ) AS vs
    WHERE vs.idx_b IS NOT NULL
),

edges AS (
    SELECT
        node_lo,
        node_hi,
        is_arc,
        min(st_length(geom)) AS edge_len
    FROM {{ ref('int__topology__edges') }}
    GROUP BY node_lo, node_hi, is_arc
),

-- a straight edge and an arc between the same nodes: the closer length wins (one
-- grouped pass; a self-join on the minimum wrecks the row estimates downstream)
direct_matches AS (
    SELECT
        st.globalid,
        st.idx_a,
        coalesce(
            min(abs(e.edge_len - st.step_len)) FILTER (WHERE e.is_arc)
            < min(abs(e.edge_len - st.step_len)) FILTER (WHERE NOT e.is_arc),
            bool_and(e.is_arc)
        ) AS is_arc,
        min(abs(e.edge_len - st.step_len)) AS len_diff
    FROM steps AS st
    INNER JOIN edges AS e
        ON
            least(st.node_a, st.node_b) = e.node_lo
            AND greatest(st.node_a, st.node_b) = e.node_hi
    WHERE
        st.node_a != st.node_b
        AND abs(e.edge_len - st.step_len) <= {{ len_tol }}
    GROUP BY st.globalid, st.idx_a
),

-- the stretch of segment a step covers, wherever there's something between its nodes
-- to compare (unsnapped vertices, an arc) or a path to search for
step_pieces AS (
    SELECT
        st.globalid,
        st.idx_a,
        st_linesubstring(
            s.geom,
            least(st.along_a / s.seg_len, 1),
            least(st.along_b / s.seg_len, 1)
        ) AS piece
    FROM steps AS st
    INNER JOIN segments AS s ON st.globalid = s.globalid
    LEFT JOIN direct_matches AS dm ON st.globalid = dm.globalid AND st.idx_a = dm.idx_a
    WHERE
        st.node_a != st.node_b
        AND (dm.globalid IS NULL OR dm.is_arc OR st.idx_b > st.idx_a + 1)
),

-- a direct match with nothing between its nodes is the same straight line: offset 0
direct_offsets AS (
    SELECT
        dm.globalid,
        dm.idx_a,
        dm.is_arc,
        dm.len_diff,
        coalesce(po.offset_ft, 0) AS offset_ft
    FROM direct_matches AS dm
    LEFT JOIN (
        SELECT
            p.globalid,
            p.idx_a,
            st_hausdorffdistance(p.piece, eg.geom) AS offset_ft
        FROM step_pieces AS p
        INNER JOIN direct_matches AS pdm ON p.globalid = pdm.globalid AND p.idx_a = pdm.idx_a
        INNER JOIN steps AS st ON p.globalid = st.globalid AND p.idx_a = st.idx_a
        -- every owner's copy of an edge is the same line
        CROSS JOIN LATERAL (
            SELECT e.geom
            FROM {{ ref('int__topology__edges') }} AS e
            WHERE
                e.node_lo = least(st.node_a, st.node_b)
                AND e.node_hi = greatest(st.node_a, st.node_b)
                AND e.is_arc = pdm.is_arc
            ORDER BY e.atomicid
            LIMIT 1
        ) AS eg
    ) AS po ON dm.globalid = po.globalid AND dm.idx_a = po.idx_a
),

-- steps with no direct match, or one too far off, get the path search
search_steps AS MATERIALIZED (
    SELECT
        st.globalid,
        st.idx_a,
        st.node_a,
        st.node_b,
        st.step_len,
        p.piece
    FROM steps AS st
    INNER JOIN step_pieces AS p ON st.globalid = p.globalid AND st.idx_a = p.idx_a
    LEFT JOIN direct_offsets AS d ON st.globalid = d.globalid AND st.idx_a = d.idx_a
    WHERE d.globalid IS NULL OR d.offset_ft > {{ max_offset }}
),

node_geoms AS MATERIALIZED (
    SELECT
        node_id,
        geom
    FROM {{ ref('int__topology__nodes') }}
    UNION ALL
    SELECT DISTINCT ON (node_id)
        node_id,
        geom
    FROM {{ ref('int__topology__edge_crossings') }}
),

-- edges in both directions, so walking a path is a plain join
adjacency AS MATERIALIZED (
    SELECT
        node_lo AS from_node,
        node_hi AS to_node,
        is_arc,
        edge_len
    FROM edges
    UNION ALL
    SELECT
        node_hi AS from_node,
        node_lo AS to_node,
        is_arc,
        edge_len
    FROM edges
),

-- Bounded walk from the step's first node, as in int__topology__edges_unnoded's orphan
-- split: never revisit a node, stop once over the length budget, and only step to
-- nodes near the segment.
path_search (
    globalid, idx_a, node_b, step_len, cur_node, path_nodes, path_arcs, path_len, hops
) AS (
    SELECT
        globalid,
        idx_a,
        node_b,
        step_len,
        node_a,
        ARRAY[node_a],
        ARRAY[]::boolean[],
        0::double precision,
        0
    FROM search_steps

    UNION ALL

    SELECT
        ps.globalid,
        ps.idx_a,
        ps.node_b,
        ps.step_len,
        a.to_node,
        ps.path_nodes || a.to_node,
        ps.path_arcs || a.is_arc,
        ps.path_len + a.edge_len,
        ps.hops + 1
    FROM path_search AS ps
    INNER JOIN adjacency AS a ON ps.cur_node = a.from_node
    INNER JOIN node_geoms AS n ON a.to_node = n.node_id
    INNER JOIN search_steps AS ss ON ps.globalid = ss.globalid AND ps.idx_a = ss.idx_a
    WHERE
        ps.hops < {{ max_hops }}
        AND ps.cur_node != ps.node_b
        AND NOT (a.to_node = any(ps.path_nodes))
        AND ps.path_len + a.edge_len <= ps.step_len + {{ len_tol }}
        AND (a.to_node = ps.node_b OR st_dwithin(n.geom, ss.piece, {{ max_offset }}))
),

path_candidates AS (
    SELECT
        ps.globalid,
        ps.idx_a,
        ps.path_nodes,
        ps.path_arcs,
        abs(ps.path_len - ps.step_len) AS len_diff,
        st_hausdorffdistance(
            ss.piece,
            (
                SELECT st_collect(e.geom)
                FROM generate_series(1, ps.hops) AS i
                CROSS JOIN LATERAL (
                    SELECT te.geom
                    FROM {{ ref('int__topology__edges') }} AS te
                    WHERE
                        te.node_lo = least(ps.path_nodes[i], ps.path_nodes[i + 1])
                        AND te.node_hi = greatest(ps.path_nodes[i], ps.path_nodes[i + 1])
                        AND te.is_arc = ps.path_arcs[i]
                    ORDER BY te.atomicid
                    LIMIT 1
                ) AS e
            )
        ) AS offset_ft
    FROM path_search AS ps
    INNER JOIN search_steps AS ss ON ps.globalid = ss.globalid AND ps.idx_a = ss.idx_a
    WHERE
        ps.cur_node = ps.node_b
        AND abs(ps.path_len - ps.step_len) <= {{ len_tol }}
),

path_matches AS (
    SELECT DISTINCT ON (globalid, idx_a)
        globalid,
        idx_a,
        path_nodes,
        path_arcs,
        len_diff,
        offset_ft
    FROM path_candidates
    WHERE offset_ft <= {{ max_offset }}
    ORDER BY globalid, idx_a, offset_ft, len_diff
),

-- a path's stretch of segment is split between its edges in proportion to their lengths
path_edges AS (
    SELECT
        pm.globalid,
        pm.idx_a,
        i AS hop,
        pm.path_nodes[i] AS from_node,
        pm.path_nodes[i + 1] AS to_node,
        pm.path_arcs[i] AS is_arc,
        pm.len_diff,
        pm.offset_ft,
        e.edge_len,
        sum(e.edge_len) OVER (PARTITION BY pm.globalid, pm.idx_a ORDER BY i) AS cum_len,
        sum(e.edge_len) OVER (PARTITION BY pm.globalid, pm.idx_a) AS total_len
    FROM path_matches AS pm
    CROSS JOIN LATERAL generate_series(1, array_length(pm.path_nodes, 1) - 1) AS i
    INNER JOIN edges AS e
        ON
            least(pm.path_nodes[i], pm.path_nodes[i + 1]) = e.node_lo
            AND greatest(pm.path_nodes[i], pm.path_nodes[i + 1]) = e.node_hi
            AND pm.path_arcs[i] = e.is_arc
),

matched AS (
    SELECT
        st.globalid,
        st.idx_a,
        'edge' AS match_type,
        1 AS hop,
        st.node_a AS from_node,
        st.node_b AS to_node,
        d.is_arc,
        st.along_a AS along_start,
        st.along_b AS along_end,
        d.len_diff,
        d.offset_ft
    FROM direct_offsets AS d
    INNER JOIN steps AS st ON d.globalid = st.globalid AND d.idx_a = st.idx_a
    WHERE d.offset_ft <= {{ max_offset }}
    UNION ALL
    SELECT
        st.globalid,
        st.idx_a,
        'path' AS match_type,
        pe.hop,
        pe.from_node,
        pe.to_node,
        pe.is_arc,
        st.along_a + st.step_len * (pe.cum_len - pe.edge_len) / pe.total_len AS along_start,
        st.along_a + st.step_len * pe.cum_len / pe.total_len AS along_end,
        pe.len_diff,
        pe.offset_ft
    FROM path_edges AS pe
    INNER JOIN steps AS st ON pe.globalid = st.globalid AND pe.idx_a = st.idx_a
)

SELECT
    st.globalid,
    st.idx_a,
    st.idx_b,
    st.node_a,
    st.node_b,
    st.step_len,
    CASE
        WHEN m.globalid IS NOT NULL THEN m.match_type
        WHEN st.node_a = st.node_b AND st.step_len <= {{ len_tol }} THEN 'same_node'
        ELSE 'none'
    END AS match_type,
    m.hop,
    m.from_node,
    m.to_node,
    m.is_arc,
    m.along_start,
    m.along_end,
    -- half-open, so exactly one matched edge holds the midpoint
    coalesce(m.along_start <= st.seg_len / 2 AND st.seg_len / 2 < m.along_end, FALSE) AS covers_midpoint,
    m.len_diff AS len_diff_ft,
    m.offset_ft
FROM steps AS st
LEFT JOIN matched AS m ON st.globalid = m.globalid AND st.idx_a = m.idx_a
