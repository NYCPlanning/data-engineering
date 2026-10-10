-- district_boundary_strip_noise_rings
CREATE OR REPLACE TABLE {name} AS
WITH owner_counts AS (
    SELECT node_lo, node_hi, is_arc, count(DISTINCT atomicid) AS total_owners
    FROM edges
    GROUP BY node_lo, node_hi, is_arc
),

one_owner_edges AS (
    SELECT e.entity_id, e.geom
    FROM {name}_edges AS e
    INNER JOIN owner_counts AS oc
        ON e.node_lo = oc.node_lo AND e.node_hi = oc.node_hi AND e.is_arc = oc.is_arc
    WHERE e.edge_type = 'exterior' AND oc.total_owners = 1
),

dumped AS (
    SELECT r.entity_id, unnest(ST_Dump(r.geom)) AS d
    FROM {name}_raw AS r
),

parts AS (
    -- ST_Dump of a single Polygon has an empty path - NULL would never join below
    SELECT entity_id, coalesce(d.path[1], 1) AS part_n, d.geom AS part
    FROM dumped
),

interior_rings AS (
    SELECT entity_id, part_n, ring_n, ST_InteriorRingN(part, ring_n) AS ring
    FROM (
        SELECT entity_id, part_n, part, unnest(generate_series(1, ST_NumInteriorRings(part))) AS ring_n
        FROM parts
    )
),

kept_rings AS (
    SELECT ir.entity_id, ir.part_n, list(ir.ring ORDER BY ir.ring_n) AS rings
    FROM interior_rings AS ir
    WHERE NOT EXISTS (
        SELECT 1 FROM one_owner_edges AS e
        WHERE e.entity_id = ir.entity_id AND ST_Covers(ir.ring, e.geom)
    )
    GROUP BY ir.entity_id, ir.part_n
),

rebuilt AS (
    SELECT
        p.entity_id,
        CASE
            WHEN k.rings IS NULL THEN ST_MakePolygon(ST_ExteriorRing(p.part))
            ELSE ST_MakePolygon(ST_ExteriorRing(p.part), k.rings)
        END AS geom
    FROM parts AS p
    LEFT JOIN kept_rings AS k ON p.entity_id = k.entity_id AND p.part_n = k.part_n
)

SELECT entity_id, {union_agg}(geom) AS geom
FROM rebuilt
GROUP BY entity_id;
