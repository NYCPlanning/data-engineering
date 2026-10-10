-- district_boundary_validity
CREATE OR REPLACE TABLE {name}_validity AS
WITH exterior AS (
    SELECT entity_id, node_lo, node_hi, geom
    FROM {name}_edges
    WHERE edge_type = 'exterior'
),

merged_check AS (
    SELECT entity_id, ST_IsSimple(ST_LineMerge(ST_Collect(list(geom)))) AS is_simple_ok
    FROM exterior
    GROUP BY entity_id
),

node_degree AS (
    SELECT entity_id, node_id, count(*) AS degree
    FROM (
        SELECT entity_id, node_lo AS node_id FROM exterior
        UNION ALL
        SELECT entity_id, node_hi AS node_id FROM exterior
    )
    GROUP BY entity_id, node_id
),

degree_check AS (
    SELECT
        entity_id,
        count(*) FILTER (WHERE degree % 2 != 0) AS n_odd_degree_nodes,
        count(*) AS n_nodes_total
    FROM node_degree
    GROUP BY entity_id
),

buildarea_check AS (
    SELECT
        entity_id,
        geom IS NOT NULL
        AND NOT ST_IsEmpty(geom)
        AND ST_GeometryType(geom)::VARCHAR IN ('POLYGON', 'MULTIPOLYGON') AS buildarea_ok
    FROM {name}_raw
),

final_geom_check AS (
    SELECT entity_id, geom IS NOT NULL AND ST_IsValid(geom) AS final_geom_valid_ok
    FROM {name}
)

SELECT
    dc.entity_id,
    dc.n_odd_degree_nodes = 0 AS even_degree_ok,
    dc.n_odd_degree_nodes,
    dc.n_nodes_total,
    coalesce(mc.is_simple_ok, false) AS is_simple_ok,
    coalesce(bc.buildarea_ok, false) AS buildarea_ok,
    coalesce(fc.final_geom_valid_ok, false) AS final_geom_valid_ok,
    (dc.n_odd_degree_nodes = 0)
    AND coalesce(bc.buildarea_ok, false)
    AND coalesce(fc.final_geom_valid_ok, false) AS is_valid
FROM degree_check AS dc
LEFT JOIN merged_check AS mc ON dc.entity_id = mc.entity_id
LEFT JOIN buildarea_check AS bc ON dc.entity_id = bc.entity_id
LEFT JOIN final_geom_check AS fc ON dc.entity_id = fc.entity_id;
