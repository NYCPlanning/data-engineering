{{
    config(
        materialized='table',
        indexes=[{'columns': ['geom'], 'type': 'gist'}],
        tags=['blocks']
    )
}}

-- Tax sub-blocks without their mostly-water lots, so those lots can't join land
-- across the water. Removing a lot can split a sub-block, so this is one row per
-- contiguous part. Sub-blocks with no mostly-water lots pass through unchanged.

WITH lots AS (
    SELECT
        tb.block_id,
        lot.bbl,
        COALESCE(w.is_mostly_water, FALSE) AS is_mostly_water
    FROM {{ ref('int_tz__tax_blocks') }} AS tb
    CROSS JOIN LATERAL UNNEST(tb.bbls) AS lot (bbl)
    LEFT JOIN {{ ref('int_blocks__lot_water_share') }} AS w ON lot.bbl = w.bbl
),

affected AS (
    SELECT DISTINCT block_id
    FROM lots
    WHERE is_mostly_water
),

unchanged AS (
    SELECT
        tb.block_id,
        1 AS part,
        tb.bbls,
        tb.geom
    FROM {{ ref('int_tz__tax_blocks') }} AS tb
    WHERE tb.block_id NOT IN (SELECT a.block_id FROM affected AS a)
),

land_lots AS (
    SELECT
        l.block_id,
        l.bbl,
        p.geom
    FROM lots AS l
    INNER JOIN {{ source('build_sources', 'pluto') }} AS p ON l.bbl = p.bbl
    WHERE
        l.block_id IN (SELECT a.block_id FROM affected AS a)
        AND NOT l.is_mostly_water
),

rebuilt AS (
    SELECT
        block_id,
        (ST_DUMP(ST_UNION(geom))).geom AS geom
    FROM land_lots
    GROUP BY block_id
),

numbered AS (
    SELECT
        block_id,
        geom,
        ROW_NUMBER() OVER (PARTITION BY block_id ORDER BY ST_AREA(geom) DESC) AS part
    FROM rebuilt
),

rebuilt_lots AS (
    SELECT
        n.block_id,
        n.part,
        n.geom,
        -- empty for slivers of a lot whose main piece is in another part
        COALESCE(ARRAY_AGG(ll.bbl) FILTER (WHERE ll.bbl IS NOT NULL), '{}') AS bbls
    FROM numbered AS n
    LEFT JOIN land_lots AS ll
        ON
            n.block_id = ll.block_id
            AND ST_WITHIN(ST_POINTONSURFACE(ll.geom), n.geom)
    GROUP BY n.block_id, n.part, n.geom
)

SELECT
    block_id,
    part,
    bbls,
    geom
FROM unchanged

UNION ALL

SELECT
    block_id,
    part,
    bbls,
    geom
FROM rebuilt_lots
