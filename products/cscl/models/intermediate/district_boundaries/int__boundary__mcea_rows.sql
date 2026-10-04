{{ config(
    materialized='table',
    indexes=[{'columns': ['entity_id']}, {'columns': ['geom'], 'type': 'gist'}]
) }}

-- MCEA boundaries grouped into the published layer's rows: one row per connected piece of
-- the MCEA's dissolved 2000 census tracts. Tract polygons include their water, so land
-- parts across a waterway share a row (e.g. MCEA 0801: three rows holding 1, 1 and 16
-- parts). The tract union only decides which parts go together; the geometry itself is
-- int__boundary__mcea's.

WITH tract_components AS (
    SELECT
        t.mcea,
        (st_dump(st_union(t.geom))).geom AS geom
    FROM {{ ref('stg__censustract2000') }} AS t
    WHERE t.mcea IS NOT NULL
    GROUP BY t.mcea
),

parts AS (
    SELECT
        b.entity_id,
        (st_dump(b.geom)).geom AS geom
    FROM {{ ref('int__boundary__mcea') }} AS b
)

SELECT
    c.mcea AS entity_id,
    st_multi(st_collect(p.geom)) AS geom
FROM tract_components AS c
INNER JOIN parts AS p
    ON c.mcea = p.entity_id AND st_intersects(c.geom, st_pointonsurface(p.geom))
GROUP BY c.mcea, c.geom
