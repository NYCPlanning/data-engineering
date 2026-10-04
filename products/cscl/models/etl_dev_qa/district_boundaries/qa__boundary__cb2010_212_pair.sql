{{ config(materialized='view') }}

-- Census blocks 10212003000 / 10212004002 (Manhattan), where the published
-- fgdb_nycb2010 is not the union of the AtomicPolygons tagged with each block: two APs
-- each cross prod's dividing line into the other block. Kept as a reference case for
-- the prod-side block/AP divergence. Layers, by `layer`:
--   prod_cb      - prod's census block polygon
--   ap           - each AtomicPolygon, with the block its own attribute says
--   ap_spillover - the part of an AP lying inside a DIFFERENT prod block than its
--                  attribute says (prod_cb = the block it actually falls in)

WITH pair AS (
    SELECT unnest(ARRAY['10212003000', '10212004002']) AS bctcb2010
),

prod AS (
    SELECT
        p.bctcb2010,
        linearize(p.shape) AS geom
    FROM {{ adapter.get_relation(database="db-cscl", schema="production_outputs", identifier="fgdb_nycb2010") }} AS p
    INNER JOIN pair ON p.bctcb2010 = pair.bctcb2010
),

aps AS (
    SELECT
        m.atomicid,
        m.bctcb2010,
        a.geom
    FROM {{ ref('int__topology__ap_entities') }} AS m
    INNER JOIN pair ON m.bctcb2010 = pair.bctcb2010
    INNER JOIN {{ ref('stg__atomicpolygons') }} AS a ON m.atomicid = a.atomicid
),

layers AS (
    SELECT
        'prod_cb' AS layer,
        NULL AS atomicid,
        NULL AS ap_cb,
        bctcb2010 AS prod_cb,
        geom
    FROM prod
    UNION ALL
    SELECT
        'ap' AS layer,
        atomicid,
        bctcb2010 AS ap_cb,
        NULL AS prod_cb,
        geom
    FROM aps
    UNION ALL
    SELECT
        'ap_spillover' AS layer,
        a.atomicid,
        a.bctcb2010 AS ap_cb,
        p.bctcb2010 AS prod_cb,
        st_intersection(a.geom, p.geom) AS geom
    FROM aps AS a
    INNER JOIN prod AS p ON a.bctcb2010 != p.bctcb2010 AND st_intersects(a.geom, p.geom)
)

SELECT
    layer,
    atomicid,
    ap_cb,
    prod_cb,
    round(st_area(geom)::numeric, 1) AS area_sqft,
    st_multi(st_collectionextract(geom, 3))::GEOMETRY (MULTIPOLYGON, 2263) AS geom
FROM layers
WHERE st_area(geom) > 1
