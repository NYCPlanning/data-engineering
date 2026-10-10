

-- 2010 census block boundaries built from the AtomicPolygon topology (macros/district_boundary.sql).

WITH owner_counts AS (
    SELECT node_lo, node_hi, is_arc, count(DISTINCT atomicid) AS total_owners
    FROM "db-cscl"."ar_cscl_districts_gdb_db_qa_oct4"."int__topology__edges"
    GROUP BY node_lo, node_hi, is_arc
),

one_owner_edges AS (
    SELECT e.entity_id, e.geom
    FROM "db-cscl"."ar_cscl_districts_gdb_db_qa_oct4"."int__boundary__cb2010_edges" AS e
    INNER JOIN owner_counts AS oc
        ON e.node_lo = oc.node_lo AND e.node_hi = oc.node_hi AND e.is_arc = oc.is_arc
    WHERE e.edge_type = 'exterior' AND oc.total_owners = 1
),

parts AS (
    SELECT
        r.entity_id,
        -- ST_Dump of a single Polygon has an empty path - NULL would never join below
        coalesce(d.path[1], 1) AS part_n,
        d.geom AS part
    FROM "db-cscl"."ar_cscl_districts_gdb_db_qa_oct4"."int__boundary__cb2010_raw" AS r
    CROSS JOIN LATERAL st_dump(r.geom) AS d
),

interior_rings AS (
    SELECT
        p.entity_id,
        p.part_n,
        n AS ring_n,
        st_interiorringn(p.part, n) AS ring
    FROM parts AS p
    CROSS JOIN LATERAL generate_series(1, st_numinteriorrings(p.part)) AS n
),

kept_rings AS (
    SELECT
        ir.entity_id,
        ir.part_n,
        array_agg(ir.ring ORDER BY ir.ring_n) AS rings
    FROM interior_rings AS ir
    WHERE NOT EXISTS (
        SELECT 1
        FROM one_owner_edges AS e
        WHERE e.entity_id = ir.entity_id AND e.geom && ir.ring AND st_covers(ir.ring, e.geom)
    )
    GROUP BY ir.entity_id, ir.part_n
),

rebuilt AS (
    SELECT
        p.entity_id,
        CASE
            WHEN k.rings IS NULL THEN st_makepolygon(st_exteriorring(p.part))
            ELSE st_makepolygon(st_exteriorring(p.part), k.rings)
        END AS geom
    FROM parts AS p
    LEFT JOIN kept_rings AS k ON p.entity_id = k.entity_id AND p.part_n = k.part_n
)

SELECT
    entity_id,
    st_union(geom) AS geom
FROM rebuilt
GROUP BY entity_id
