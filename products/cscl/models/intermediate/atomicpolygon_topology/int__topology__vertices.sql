{{ config(
    materialized='table'
) }}

-- Every ring control vertex of every in-scope AtomicPolygon, in ring order. The source
-- geometry (stg__atomicpolygons.raw_geom) is already linear: each circular arc was
-- expanded into many vertices at load time, and the arc itself is recorded in
-- dcp_cscl_atomicpolygons_arcs (part, ring, seq, v0/v1 = first/last vertex index of the
-- arc within its ring, and the three stored control points). Here the expansion is
-- undone so downstream sees what it always did - the stored control points: vertices
-- strictly inside an arc (v0 < i < v1) are dropped and the arc's middle control point is
-- re-inserted between its endpoints, flagged is_arc_mid. (part, ring) is the ring key -
-- rings of multi-part or holed AtomicPolygons must never be interleaved.
--
-- is_arc_mid flags the middle control point of each circular arc. Mid points are not
-- topology nodes: an arc's mid can sit anywhere on the arc, so two AtomicPolygons tracing
-- the same arc often disagree on it.

WITH dumped_raw AS (
    -- select-list unnest: a lateral unnest would copy the (large) polygon per vertex
    SELECT
        a.ogc_fid,
        a.atomicid,
        a.water_flag,
        unnest(dump_points(a.raw_geom)) AS d
    FROM {{ ref('int__topology__ap_entities') }} AS s
    INNER JOIN {{ ref('stg__atomicpolygons') }} AS a ON s.atomicid = a.atomicid
),

dumped AS (
    SELECT
        ogc_fid,
        atomicid,
        water_flag,
        d.part,
        d.ring,
        d.point - 1 AS vertex_idx,
        d.geom
    FROM dumped_raw
),

arcs AS (
    SELECT
        ogc_fid,
        part + 1 AS part,
        ring + 1 AS ring,
        v0,
        v1,
        x1,
        y1
    FROM {{ source('recipe_sources', 'dcp_cscl_atomicpolygons_arcs') }}
),

control_points AS (
    SELECT
        d.atomicid,
        d.water_flag,
        d.part,
        d.ring,
        d.vertex_idx::NUMERIC AS ord,
        FALSE AS is_arc_mid,
        d.geom
    FROM dumped AS d
    WHERE NOT EXISTS (
        SELECT 1
        FROM arcs AS x
        WHERE
            x.ogc_fid = d.ogc_fid AND x.part = d.part AND x.ring = d.ring
            AND d.vertex_idx > x.v0 AND d.vertex_idx < x.v1
    )
    UNION ALL
    SELECT
        a.atomicid,
        a.water_flag,
        x.part,
        x.ring,
        x.v0 + 0.5 AS ord,
        TRUE AS is_arc_mid,
        st_point(x.x1, x.y1) AS geom
    FROM arcs AS x
    INNER JOIN {{ ref('stg__atomicpolygons') }} AS a ON x.ogc_fid = a.ogc_fid
    INNER JOIN {{ ref('int__topology__ap_entities') }} AS s ON a.atomicid = s.atomicid
),

deduped AS (
    SELECT
        atomicid,
        water_flag,
        part,
        ring,
        ord,
        is_arc_mid,
        geom,
        lag(geom) OVER (PARTITION BY atomicid, part, ring ORDER BY ord) AS prev_geom
    FROM control_points
)

SELECT
    atomicid,
    water_flag,
    part,
    ring,
    row_number() OVER (PARTITION BY atomicid, part, ring ORDER BY ord) AS seq,
    is_arc_mid,
    geom
FROM deduped
WHERE prev_geom IS NULL OR NOT st_equals(geom, prev_geom)
