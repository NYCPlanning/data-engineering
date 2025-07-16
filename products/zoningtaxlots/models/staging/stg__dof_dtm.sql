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
        coalesce(boro::text, left(bbl::text, 1)) AS boro,
        coalesce(block::text, substring(bbl::text, 2, 5)) AS block,
        coalesce(lot::text, substring(bbl::text, 7, 4)) AS lot,
        wkb_geometry AS geom
    FROM dof_dtm
)

SELECT
    row_number() OVER () AS dtm_id,
    bbl,
    boro,
    block,
    lot,
    ST_Multi(ST_Union(ST_MakeValid(geom))) AS geom
FROM coalesced
GROUP BY bbl, boro, block, lot
