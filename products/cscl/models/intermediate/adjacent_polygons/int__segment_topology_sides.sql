{{ config(
    materialized = 'table'
) }}

-- Left/right AtomicPolygon of every segment from the AP topology: the owners of the
-- topology edge the segment runs along at its midpoint, falling back to the offset probe
-- where it runs along none. int__segment_atomicpolygons uses this pair when
-- fix_lion_lr_from_topology is on (see dbt_project.yml).
--
-- Why: the 2 ft probe (int__segment_offsets) lands in the AP behind the adjacent one
-- wherever that AP is under 2 ft deep at the segment's midpoint (corners, node pinches,
-- sliver APs; e.g. segmentid 161961). Edge ownership has no depth.
--
-- A segment matches if the step holding its midpoint matched an edge
-- (int__segment_topology_edge_matches): its left/right APs are that edge's owners, placed
-- by int__topology__edge_sides crossed with the direction the segment walks the edge.
-- The midpoint is where the probe looks too, so the rest of the segment needn't match
-- (e.g. it ends mid-edge, off any node).
--
-- Everything else takes the perpendicular-at-midpoint probe at
-- lion_lr_fallback_probe_offset_ft: unmatched segments on both sides, and a matched
-- segment on a side with no owner (a one-owner edge: shoreline, edge of scope) -
-- whatever lies across, usually nothing. See dbt_project.yml for why that probe is 2 ft
-- too: unmatched segments between two APs mostly sit just beside the boundary.
--
-- method:
--   edges          - matched (a side may still come from the probe; see *_source)
--   probe_fallback - not matched: dangles, alleys and driveways inside one AP, and
--                    segments whose midpoint step doesn't match an edge
--   out_of_scope   - an assigned AP is outside the topology scope (int__topology__ap_entities):
--                    the 2 ft probe pair, unchanged
--   bad_geom       - geometry outside the AP extent, e.g. segmentid 358043, a corrupt arc
--                    that linearizes to a ~3M ft circle: the 2 ft probe pair, unchanged

{% set probe_offset = var('lion_lr_fallback_probe_offset_ft') %}

WITH ap_extent AS (
    SELECT st_extent_agg(geom) AS geom
    FROM {{ ref('stg__atomicpolygons') }}
),

segments AS (
    SELECT
        s.globalid,
        s.segmentid,
        s.geom,
        st_npoints(s.geom) AS n_points,
        -- bounding box contained by the extent's
        not(
            st_xmin(s.geom) >= st_xmin(e.geom) AND st_xmax(s.geom) <= st_xmax(e.geom)
            AND st_ymin(s.geom) >= st_ymin(e.geom) AND st_ymax(s.geom) <= st_ymax(e.geom)
        ) AS is_bad_geom
    FROM {{ ref('int__segments') }} AS s
    CROSS JOIN ap_extent AS e
),

-- owners of every matched edge, placed left/right of the segment
edge_owners AS (
    SELECT
        m.globalid,
        m.covers_midpoint,
        es.atomicid,
        es.owner_left = (m.from_node < m.to_node) AS is_left
    FROM {{ ref('int__segment_topology_edge_matches') }} AS m
    INNER JOIN {{ ref('int__topology__edge_sides') }} AS es
        ON
            least(m.from_node, m.to_node) = es.node_lo
            AND greatest(m.from_node, m.to_node) = es.node_hi
            AND m.is_arc = es.is_arc
    WHERE es.owner_left IS NOT NULL
),

owners AS (
    SELECT
        globalid,
        bool_or(covers_midpoint) AS has_midpoint_edge,
        min(atomicid) FILTER (WHERE covers_midpoint AND is_left) AS left_atomicid,
        min(atomicid) FILTER (WHERE covers_midpoint AND NOT is_left) AS right_atomicid,
        count(DISTINCT atomicid) FILTER (WHERE is_left) AS n_left_aps,
        count(DISTINCT atomicid) FILTER (WHERE NOT is_left) AS n_right_aps
    FROM edge_owners
    GROUP BY globalid
),

match_summary AS (
    SELECT
        m.globalid,
        bool_or(m.idx_a = 1) AS starts_on_node,
        bool_or(m.idx_b = s.n_points) AS ends_on_node,
        count(DISTINCT m.idx_a) FILTER (WHERE m.match_type = 'none') AS n_unmatched_steps,
        count(*) FILTER (WHERE m.match_type IN ('edge', 'path')) AS n_edges,
        count(*) FILTER (WHERE m.match_type = 'path') AS n_path_edges,
        max(m.len_diff_ft) AS max_len_diff_ft,
        max(m.offset_ft) AS max_offset_ft
    FROM {{ ref('int__segment_topology_edge_matches') }} AS m
    INNER JOIN segments AS s ON m.globalid = s.globalid
    GROUP BY m.globalid
),

matched AS (
    SELECT
        s.globalid,
        s.segmentid,
        s.geom,
        s.is_bad_geom,
        coalesce(ms.starts_on_node, FALSE) AS starts_on_node,
        coalesce(ms.ends_on_node, FALSE) AS ends_on_node,
        coalesce(o.has_midpoint_edge, FALSE) AS is_matched,
        ms.n_unmatched_steps,
        ms.n_edges,
        ms.n_path_edges,
        ms.max_len_diff_ft,
        ms.max_offset_ft,
        o.left_atomicid,
        o.right_atomicid,
        o.n_left_aps,
        o.n_right_aps
    FROM segments AS s
    LEFT JOIN match_summary AS ms ON s.globalid = ms.globalid
    LEFT JOIN owners AS o ON s.globalid = o.globalid
),

-- offset_points needs a LineString: a multi-part line probes its longest part
probe_lines AS (
    SELECT
        globalid,
        arg_max(part, st_length(part)) AS line
    FROM (
        SELECT
            m.globalid,
            unnest(st_dump(st_linemerge(m.geom))).geom AS part
        FROM matched AS m
        WHERE
            NOT m.is_bad_geom
            AND (NOT m.is_matched OR m.left_atomicid IS NULL OR m.right_atomicid IS NULL)
    ) AS parts
    GROUP BY globalid
),

probe_points AS (
    SELECT
        globalid,
        o.left_offset_point AS left_point,
        o.right_offset_point AS right_point
    FROM (
        SELECT
            pl.globalid,
            offset_points(pl.line, offset_length := {{ probe_offset }}) AS o
        FROM probe_lines AS pl
    ) AS offsets
),

probed AS (
    SELECT
        pp.globalid,
        lp.atomicid AS left_atomicid,
        rp.atomicid AS right_atomicid
    FROM probe_points AS pp
    LEFT JOIN {{ ref("stg__atomicpolygons") }} AS lp ON st_within(pp.left_point, lp.geom)
    LEFT JOIN {{ ref("stg__atomicpolygons") }} AS rp ON st_within(pp.right_point, rp.geom)
),

assigned AS (
    SELECT
        m.*,
        m.is_matched AND m.left_atomicid IS NOT NULL AS left_from_edge,
        m.is_matched AND m.right_atomicid IS NOT NULL AS right_from_edge,
        -- a one-owner edge has its owner on the other side only, so a probe that lands
        -- in that same AP (a centerline just off the boundary, on the owner's side)
        -- means nothing is across
        CASE
            WHEN m.is_matched AND m.left_atomicid IS NOT NULL THEN m.left_atomicid
            WHEN m.is_matched AND p.left_atomicid = m.right_atomicid THEN NULL
            ELSE p.left_atomicid
        END AS assigned_left,
        CASE
            WHEN m.is_matched AND m.right_atomicid IS NOT NULL THEN m.right_atomicid
            WHEN m.is_matched AND p.right_atomicid = m.left_atomicid THEN NULL
            ELSE p.right_atomicid
        END AS assigned_right
    FROM matched AS m
    LEFT JOIN probed AS p ON m.globalid = p.globalid
),

-- Scope: edge owners are always in scope, but a probe can land outside it. Either way
-- the segment keeps today's 2 ft probe answer.
scoped AS (
    SELECT
        a.*,
        CASE
            WHEN a.is_bad_geom THEN 'bad_geom'
            WHEN
                (a.assigned_left IS NOT NULL AND le.atomicid IS NULL)
                OR (a.assigned_right IS NOT NULL AND re.atomicid IS NULL)
                THEN 'out_of_scope'
            WHEN a.is_matched THEN 'edges'
            ELSE 'probe_fallback'
        END AS method
    FROM assigned AS a
    LEFT JOIN {{ ref('int__topology__ap_entities') }} AS le ON a.assigned_left = le.atomicid
    LEFT JOIN {{ ref('int__topology__ap_entities') }} AS re ON a.assigned_right = re.atomicid
),

probe_2ft AS (
    SELECT
        so.globalid,
        lp.atomicid AS left_atomicid,
        rp.atomicid AS right_atomicid
    FROM scoped AS sc
    INNER JOIN {{ ref('int__segment_offsets') }} AS so ON sc.globalid = so.globalid
    LEFT JOIN {{ ref("stg__atomicpolygons") }} AS lp ON st_within(so.left_offset_point, lp.geom)
    LEFT JOIN {{ ref("stg__atomicpolygons") }} AS rp ON st_within(so.right_offset_point, rp.geom)
    WHERE sc.method IN ('out_of_scope', 'bad_geom')
)

SELECT
    sc.globalid,
    sc.segmentid,
    sc.method,
    CASE WHEN p2.globalid IS NOT NULL THEN p2.left_atomicid ELSE sc.assigned_left END AS topo_left_atomicid,
    CASE WHEN p2.globalid IS NOT NULL THEN p2.right_atomicid ELSE sc.assigned_right END AS topo_right_atomicid,
    CASE
        WHEN p2.globalid IS NOT NULL THEN 'probe_2ft'
        WHEN sc.left_from_edge THEN 'edge'
        ELSE 'probe'
    END AS topo_left_source,
    CASE
        WHEN p2.globalid IS NOT NULL THEN 'probe_2ft'
        WHEN sc.right_from_edge THEN 'edge'
        ELSE 'probe'
    END AS topo_right_source,
    -- diagnostics
    sc.starts_on_node,
    sc.ends_on_node,
    sc.n_unmatched_steps,
    sc.n_edges,
    sc.n_path_edges,
    sc.max_len_diff_ft,
    sc.max_offset_ft,
    sc.n_left_aps,
    sc.n_right_aps
FROM scoped AS sc
LEFT JOIN probe_2ft AS p2 ON sc.globalid = p2.globalid
