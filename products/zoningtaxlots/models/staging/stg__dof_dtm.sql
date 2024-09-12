{{ config(
    materialized = 'table',
    indexes=[
        {'columns': ['dtm_id'], 'unique': True},
        {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

WITH dof_dtm AS (
    SELECT * FROM {{ source('recipe_sources', 'dof_dtm') }}
),

coalesced AS (
    SELECT
        bbl,
        COALESCE(boro::text, LEFT(bbl::text, 1)) AS boro,
        COALESCE(block::text, SUBSTRING(bbl::text, 2, 5)) AS block,
        COALESCE(lot::text, SUBSTRING(bbl::text, 7, 4)) AS lot,
        wkb_geometry AS geom
    FROM dof_dtm
)

SELECT
    ROW_NUMBER() OVER () AS dtm_id,
    bbl,
    boro,
    block,
    lot,
    ST_MULTI(ST_UNION(ST_MAKEVALID(geom))) AS geom
FROM coalesced
GROUP BY bbl, boro, block, lot
