

-- Every topology edge classified relative to each 2010 census block that owns it. Land only.

WITH edges AS (
    SELECT
        e.node_lo, e.node_hi, e.is_arc, e.geom, e.atomicid, m.bctcb2010 AS entity_id,
        e.water_flag = '1' AS is_water
    FROM "db-cscl"."ar_cscl_districts_gdb_db_qa_oct4"."int__topology__edges" AS e
    INNER JOIN "db-cscl"."ar_cscl_districts_gdb_db_qa_oct4"."int__topology__ap_entities" AS m ON e.atomicid = m.atomicid
),

has_water_neighbor AS (
    SELECT DISTINCT node_lo, node_hi, is_arc
    FROM edges
    WHERE is_water
),

land_land_edges AS (
    SELECT e.node_lo, e.node_hi, e.is_arc, e.geom, e.atomicid, e.entity_id
    FROM edges AS e
    LEFT JOIN has_water_neighbor AS hw
        ON e.node_lo = hw.node_lo AND e.node_hi = hw.node_hi AND e.is_arc = hw.is_arc
    WHERE NOT e.is_water AND hw.node_lo IS NULL
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
        (array_agg(geom))[1] AS geom
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

-- Emitted once per LAND-side row, using THAT row's own entity - "is this land parcel's
-- edge exterior here because it borders water" doesn't depend on which entity the
-- water itself is tagged with, so this intentionally does NOT require the land and
-- water owner to share an entity.
shoreline_edges AS (
    SELECT
        e.entity_id,
        e.node_lo,
        e.node_hi,
        e.is_arc,
        e.geom,
        'exterior' AS edge_type
    FROM edges AS e
    INNER JOIN has_water_neighbor AS hw
        ON e.node_lo = hw.node_lo AND e.node_hi = hw.node_hi AND e.is_arc = hw.is_arc
    WHERE NOT e.is_water
)

-- An AP with no entity (e.g. no fire division) still counts as an owner above, so its
-- neighbors' shared edges correctly read as 'exterior' - it just builds no boundary.
SELECT * FROM land_land_classified WHERE entity_id IS NOT NULL
UNION ALL
SELECT * FROM shoreline_edges WHERE entity_id IS NOT NULL
