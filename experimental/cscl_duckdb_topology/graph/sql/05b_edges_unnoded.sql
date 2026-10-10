-- Second half of int__topology__edges_unnoded.
--
-- Arcs: DuckDB spatial has no curve types (no CIRCULARSTRING, no ST_CurveToLine), so the
-- canonical linearized curve is carried over from PG (arc_lines, keyed by its exact
-- endpoint coordinates = the lo/hi node coordinates). Keys (node_lo, node_hi, is_arc)
-- and the choice of canonical owner are still computed here; only the curve's vertex
-- list comes from PG. arc_reconstruct.sql checks rebuilding it from the 3 control points
-- in DuckDB instead.
WITH RECURSIVE arc_keys AS (
    SELECT DISTINCT ON (least(node_a, node_b), greatest(node_a, node_b))
        least(node_a, node_b) AS node_lo,
        greatest(node_a, node_b) AS node_hi,
        CASE WHEN node_a < node_b THEN a_x ELSE b_x END AS lo_x,
        CASE WHEN node_a < node_b THEN a_y ELSE b_y END AS lo_y,
        CASE WHEN node_a < node_b THEN b_x ELSE a_x END AS hi_x,
        CASE WHEN node_a < node_b THEN b_y ELSE a_y END AS hi_y
    FROM raw_edges
    WHERE is_arc AND node_b IS NOT NULL AND node_a != node_b
    ORDER BY least(node_a, node_b), greatest(node_a, node_b), atomicid
),

arc_geoms AS (
    SELECT
        k.node_lo,
        k.node_hi,
        st_geomfromwkb(l.wkb) AS geom
    FROM arc_keys AS k
    LEFT JOIN arc_lines AS l
        ON k.lo_x = l.lo_x AND k.lo_y = l.lo_y AND k.hi_x = l.hi_x AND k.hi_y = l.hi_y
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
        st_distance(r.geom_a, r.geom_b) AS chord_length,
        coalesce(ag.geom, st_makeline(r.geom_a, r.geom_b)) AS geom
    FROM raw_edges AS r
    LEFT JOIN arc_geoms AS ag
        -- one-sided `r.is_arc` folded into the key, as in 05a_raw_edges.sql
        ON
            (CASE WHEN r.is_arc THEN least(r.node_a, r.node_b) END) = ag.node_lo
            AND greatest(r.node_a, r.node_b) = ag.node_hi
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
    WHERE t.total_owners = 1 AND NOT e.is_arc
),

directed_edges AS (
    SELECT node_lo AS from_node, node_hi AS to_node, geom, chord_length AS len
    FROM edges
    WHERE NOT is_arc
    UNION ALL
    SELECT node_hi AS from_node, node_lo AS to_node, geom, chord_length AS len
    FROM edges
    WHERE NOT is_arc
),

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
        [node_lo] AS path_nodes,
        []::geometry[] AS path_geoms,
        0.0::double AS path_length,
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
        list_append(ps.path_nodes, de.to_node),
        list_append(ps.path_geoms, de.geom),
        ps.path_length + de.len,
        ps.hops + 1
    FROM path_search AS ps
    INNER JOIN directed_edges AS de ON ps.cur_node = de.from_node
    WHERE
        ps.hops < ${max_hops}
        AND NOT list_contains(ps.path_nodes, de.to_node)
        AND NOT (ps.hops = 0 AND de.to_node = ps.b)
        AND ps.path_length + de.len <= ps.direct_length + ${split_tol}
),

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
        AND abs(path_length - direct_length) <= ${split_tol}
    ORDER BY a, b, abs(path_length - direct_length)
),

split_sub_edges AS (
    SELECT
        sm.atomicid,
        sm.water_flag,
        least(sm.path_nodes[i], sm.path_nodes[i + 1]) AS node_lo,
        greatest(sm.path_nodes[i], sm.path_nodes[i + 1]) AS node_hi,
        false AS is_arc,
        sm.path_geoms[i] AS geom
    FROM split_matches AS sm
    CROSS JOIN generate_series(1, len(sm.path_nodes) - 1) AS s (i)
),

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
        -- one-sided `NOT e.is_arc` folded into the key, as in 05a_raw_edges.sql
        ON
            (CASE WHEN NOT e.is_arc THEN e.atomicid END) = sm.atomicid
            AND e.node_lo = least(sm.a, sm.b) AND e.node_hi = greatest(sm.a, sm.b)
    WHERE sm.a IS NULL
    UNION ALL
    SELECT atomicid, water_flag, node_lo, node_hi, is_arc, geom
    FROM split_sub_edges
)

SELECT node_lo, node_hi, is_arc, geom, atomicid, water_flag
FROM edges_densified
