{{
    config(
        materialized='table',
        indexes=[{'columns': ['city_block_id'], 'unique': True}],
        tags=['blocks']
    )
}}

-- City blocks: land parts of tax sub-blocks merged wherever they share a boundary
-- edge, across tax blocks (and boroughs). Each mostly-water lot is its own row.

WITH clustered AS (
    -- groups of parts that touch at all, so each union below stays small
    SELECT
        *,
        ST_CLUSTERDBSCAN(geom, eps => 0, minpoints => 1) OVER () AS cluster_id
    FROM {{ ref('int_blocks__land_parts') }}
),

merged AS (
    -- the union merges shared edges; parts touching only at a point stay separate
    SELECT
        cluster_id,
        (ST_DUMP(ST_UNION(geom))).geom AS geom
    FROM clustered
    GROUP BY cluster_id
),

numbered AS (
    SELECT
        ROW_NUMBER() OVER () AS merged_id,
        cluster_id,
        geom
    FROM merged
),

lots AS (
    SELECT
        n.merged_id,
        lot.bbl
    FROM clustered AS c
    INNER JOIN numbered AS n
        ON
            c.cluster_id = n.cluster_id
            AND ST_WITHIN(ST_POINTONSURFACE(c.geom), n.geom)
    CROSS JOIN LATERAL UNNEST(c.bbls) AS lot (bbl)
),

land AS (
    SELECT
        l.merged_id,
        -- named after its lowest BBL, which is unique since each lot is in one row
        MIN(l.bbl) AS city_block_id,
        -- borough and block are the first six digits of a BBL
        COUNT(DISTINCT LEFT(l.bbl, 6)) AS tax_block_count,
        ARRAY_AGG(l.bbl ORDER BY l.bbl) AS bbls,
        SUM(w.water_area) / NULLIF(SUM(w.lot_area), 0) AS water_share
    FROM lots AS l
    LEFT JOIN {{ ref('int_blocks__lot_water_share') }} AS w ON l.bbl = w.bbl
    GROUP BY l.merged_id
)

SELECT
    land.city_block_id,
    FALSE AS is_mostly_water,
    land.tax_block_count,
    land.tax_block_count > 1 AS has_multiple_tax_blocks,
    land.water_share,
    land.bbls,
    n.geom
FROM numbered AS n
-- left join so a city block no part landed in fails the not_null tests
LEFT JOIN land ON n.merged_id = land.merged_id

UNION ALL

SELECT
    w.bbl AS city_block_id,
    TRUE AS is_mostly_water,
    1 AS tax_block_count,
    FALSE AS has_multiple_tax_blocks,
    w.water_share,
    ARRAY[w.bbl] AS bbls,
    p.geom
FROM {{ ref('int_blocks__lot_water_share') }} AS w
INNER JOIN {{ source('build_sources', 'pluto') }} AS p ON w.bbl = p.bbl
WHERE
    w.is_mostly_water
    -- only lots that are in a tax sub-block, like the land rows
    AND w.bbl IN (
        SELECT lot.bbl
        FROM {{ ref('int_tz__tax_blocks') }} AS tb
        CROSS JOIN LATERAL UNNEST(tb.bbls) AS lot (bbl)
    )
