{{
    config(
        materialized='table',
        indexes=[{'columns': ['geom'], 'type': 'gist'}],
        tags=['pluto_enrichment']
    )
}}

-- Create sub-blocks from tax blocks, handling non-contiguous blocks
-- Splits blocks into contiguous parts and assigns lots to their sub-block

WITH block_unions AS (
    SELECT
        borough,
        block,
        ST_UNION(p.geom) AS geom,
        ARRAY_AGG(bbl) AS all_bbls
    FROM {{ target.schema }}.pluto AS p
    GROUP BY p.borough, p.block
),

block_parts AS (
    SELECT
        borough,
        block,
        all_bbls,
        (ST_DUMP(geom)).geom AS geom
    FROM block_unions
),

numbered_parts AS (
    SELECT
        borough,
        block,
        all_bbls,
        geom,
        ROW_NUMBER() OVER (
            PARTITION BY borough, block
            ORDER BY ST_AREA(geom) DESC
        ) AS sub_block
    FROM block_parts
),

reassigned_bbls AS (
    SELECT
        np.borough,
        np.block,
        np.sub_block,
        np.geom,
        ARRAY_AGG(p.bbl) AS bbls
    FROM numbered_parts AS np
    INNER JOIN {{ target.schema }}.pluto AS p
        ON
            np.borough = p.borough
            AND np.block = p.block
            AND ST_WITHIN(ST_POINTONSURFACE(p.geom), np.geom)
    GROUP BY np.borough, np.block, np.sub_block, np.geom
)

SELECT
    borough,
    block,
    sub_block,
    borough || '-' || block || '-' || sub_block AS block_id,
    geom,
    bbls
FROM reassigned_bbls
