-- district_boundary_classify_edges (land only: include_assigned_water=false)
CREATE OR REPLACE TABLE {name}_edges AS
WITH edges_e AS (
    SELECT
        e.node_lo, e.node_hi, e.is_arc, e.geom, e.atomicid, m.{entity_column} AS entity_id,
        e.water_flag = '1' AS is_water
    FROM edges AS e
    INNER JOIN ap_entities AS m ON e.atomicid = m.atomicid
),

has_water_neighbor AS (
    SELECT DISTINCT node_lo, node_hi, is_arc
    FROM edges_e
    WHERE is_water
),

land_land_edges AS (
    SELECT e.node_lo, e.node_hi, e.is_arc, e.geom, e.atomicid, e.entity_id
    FROM edges_e AS e
    ANTI JOIN has_water_neighbor AS hw
        ON e.node_lo = hw.node_lo AND e.node_hi = hw.node_hi AND e.is_arc = hw.is_arc
    WHERE NOT e.is_water
),

edge_owner_counts AS (
    SELECT node_lo, node_hi, is_arc, count(DISTINCT atomicid) AS total_owners
    FROM land_land_edges
    GROUP BY node_lo, node_hi, is_arc
),

edge_entity_counts AS (
    SELECT
        node_lo, node_hi, is_arc, entity_id,
        count(DISTINCT atomicid) AS owners_in_entity,
        any_value(geom) AS geom
    FROM land_land_edges
    GROUP BY node_lo, node_hi, is_arc, entity_id
),

land_land_classified AS (
    SELECT
        ec.entity_id, ec.node_lo, ec.node_hi, ec.is_arc, ec.geom,
        CASE
            WHEN ec.owners_in_entity = eoc.total_owners AND eoc.total_owners >= 2 THEN 'interior'
            ELSE 'exterior'
        END AS edge_type
    FROM edge_entity_counts AS ec
    INNER JOIN edge_owner_counts AS eoc
        ON ec.node_lo = eoc.node_lo AND ec.node_hi = eoc.node_hi AND ec.is_arc = eoc.is_arc
),

shoreline_edges AS (
    SELECT e.entity_id, e.node_lo, e.node_hi, e.is_arc, e.geom, 'exterior' AS edge_type
    FROM edges_e AS e
    SEMI JOIN has_water_neighbor AS hw
        ON e.node_lo = hw.node_lo AND e.node_hi = hw.node_hi AND e.is_arc = hw.is_arc
    WHERE NOT e.is_water
)

SELECT * FROM land_land_classified WHERE entity_id IS NOT NULL
UNION ALL
SELECT * FROM shoreline_edges WHERE entity_id IS NOT NULL;
