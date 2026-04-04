{{
    config(
        materialized='table',
        tags=['pluto_enrichment']
    )
}}

-- Calculate transit zone coverage per block with ranking

WITH block_to_tz AS (
    SELECT
        tb.borough,
        tb.block,
        tb.sub_block,
        tb.geom,
        tb.bbls,
        t.transit_zone,
        ST_AREA(ST_INTERSECTION(tb.geom, ST_UNION(t.wkb_geometry))) / ST_AREA(tb.geom) * 100.0 AS pct_covered
    FROM {{ ref('int_tz__tax_blocks') }} AS tb
    INNER JOIN {{ ref('int_tz__atomic_geoms') }} AS t
        ON ST_INTERSECTS(tb.geom, t.wkb_geometry)
    GROUP BY tb.borough, tb.block, tb.sub_block, tb.geom, tb.bbls, t.transit_zone
)

SELECT
    'block' AS assignment_type,
    borough || '-' || block || '-' || sub_block AS id,
    borough,
    block,
    geom,
    sub_block,
    bbls,
    transit_zone,
    pct_covered,
    ROW_NUMBER() OVER (
        PARTITION BY borough, block, sub_block
        ORDER BY pct_covered DESC
    ) AS tz_rank
FROM block_to_tz
