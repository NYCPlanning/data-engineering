{{
    config(
        materialized='table',
        tags=['pluto_enrichment']
    )
}}

-- Lot-level assignments for ambiguous blocks
-- Only for blocks where multiple transit zones have >10% coverage

WITH ambiguous_bbls AS (
    SELECT
        UNNEST(bbls) AS bbl,
        borough,
        block,
        sub_block
    FROM {{ ref('int_tz__block_to_tz_ranked') }} AS tza
    WHERE
        tza.tz_rank > 1
        AND tza.pct_covered > 10
),

lot_to_tz AS (
    SELECT
        p.bbl,
        p.borough,
        p.block,
        t.transit_zone,
        p.geom,
        ST_AREA(ST_INTERSECTION(p.geom, ST_UNION(t.wkb_geometry))) / ST_AREA(p.geom) * 100.0 AS pct_covered
    FROM {{ target.schema }}.pluto AS p
    INNER JOIN ambiguous_bbls AS ab ON p.bbl = ab.bbl
    INNER JOIN {{ ref('int_tz__atomic_geoms') }} AS t
        ON ST_INTERSECTS(p.geom, t.wkb_geometry)
    GROUP BY p.bbl, p.borough, p.block, t.transit_zone, p.geom
),

lot_to_tz_with_rank AS (
    SELECT
        lt.*,
        tzr.tz_rank AS priority_rank
    FROM lot_to_tz AS lt
    LEFT JOIN {{ ref('dcp_transit_zone_ranks') }} AS tzr
        ON lt.transit_zone = tzr.tz_name
),

filtered_zones AS (
    SELECT
        ltr.*,
        NOT COALESCE(
            EXISTS (
                SELECT 1
                FROM lot_to_tz_with_rank AS inner_ltr
                WHERE
                    inner_ltr.bbl = ltr.bbl
                    AND inner_ltr.priority_rank < 4
            ) AND ltr.priority_rank = 4,
            FALSE
        ) AS include_zone
    FROM lot_to_tz_with_rank AS ltr
)

SELECT
    'lot' AS assignment_type,
    bbl::text AS id,
    borough,
    block,
    geom,
    1 AS sub_block,
    ARRAY[bbl] AS bbls,
    transit_zone,
    pct_covered,
    ROW_NUMBER() OVER (
        PARTITION BY bbl
        ORDER BY pct_covered DESC
    ) AS tz_rank
FROM filtered_zones
WHERE include_zone = TRUE
