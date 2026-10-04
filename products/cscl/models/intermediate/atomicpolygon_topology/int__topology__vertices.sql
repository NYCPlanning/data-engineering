{{ config(
    materialized='table'
) }}

-- Every ring vertex of every in-scope AtomicPolygon, in ring order, from the RAW curved
-- source geometry (not the linearized stg__atomicpolygons.geom) - arcs are handled
-- exactly in int__topology__edges rather than approximated here.
--
-- ST_DumpPoints on a curved MultiSurface returns [part, ring, curve_component,
-- point_in_component]; consecutive components repeat their shared transition point,
-- which is dropped so every ring is a clean ordered sequence. (part, ring) is the ring
-- key - rings of multi-part or holed AtomicPolygons must never be interleaved.
--
-- is_arc_mid flags the middle control point of each circular arc (even-indexed points
-- of a CIRCULARSTRING). Mid points are not topology nodes: an arc's mid can sit
-- anywhere on the arc, so two AtomicPolygons tracing the same arc often disagree on it.

WITH dumped AS (
    SELECT
        a.atomicid,
        a.water_flag,
        st_hasarc(a.raw_geom) AS has_arc,
        a.raw_geom,
        d.path,
        d.geom
    FROM {{ ref('int__topology__ap_entities') }} AS s
    INNER JOIN {{ ref('stg__atomicpolygons') }} AS a ON s.atomicid = a.atomicid
    CROSS JOIN LATERAL st_dumppoints(a.raw_geom) AS d
),

-- The ring a point sits on - only needed (and only computed) for AtomicPolygons with arcs.
with_ring AS (
    SELECT
        atomicid,
        water_flag,
        path,
        geom,
        CASE
            WHEN NOT has_arc THEN NULL
            WHEN path[2] = 1 THEN st_exteriorring(st_geometryn(raw_geom, path[1]))
            ELSE st_interiorringn(st_geometryn(raw_geom, path[1]), path[2] - 1)
        END AS ring_geom
    FROM dumped
),

flat AS (
    SELECT
        atomicid,
        water_flag,
        path[1] AS part,
        path[2] AS ring,
        row_number() OVER (PARTITION BY atomicid, path[1], path[2] ORDER BY path[3], path[4]) AS seq,
        coalesce(
            CASE
                -- COMPOUNDCURVE ring: path = [part, ring, component, point_in_component]
                WHEN array_length(path, 1) = 4
                    THEN st_geometrytype(st_curven(ring_geom, path[3])) = 'ST_CircularString' AND path[4] % 2 = 0
                -- bare CIRCULARSTRING ring: path = [part, ring, point]
                ELSE st_geometrytype(ring_geom) = 'ST_CircularString' AND path[3] % 2 = 0
            END,
            FALSE
        ) AS is_arc_mid,
        geom
    FROM with_ring
),

deduped AS (
    SELECT
        atomicid,
        water_flag,
        part,
        ring,
        seq,
        is_arc_mid,
        geom,
        lag(geom) OVER (PARTITION BY atomicid, part, ring ORDER BY seq) AS prev_geom
    FROM flat
)

SELECT
    atomicid,
    water_flag,
    part,
    ring,
    row_number() OVER (PARTITION BY atomicid, part, ring ORDER BY seq) AS seq,
    is_arc_mid,
    geom
FROM deduped
WHERE prev_geom IS NULL OR NOT st_equals(geom, prev_geom)
