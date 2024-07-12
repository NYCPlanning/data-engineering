{{ config(
    materialized = 'table',
    indexes=[
        {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

WITH dof_dtm AS (
    SELECT * FROM {{ source('recipe_sources', 'dof_dtm') }}
),

coalesced AS (
    SELECT
        bbl,
        wkb_geometry AS geom,
        COALESCE(boro::text, LEFT(bbl::text, 1)) AS boro,
        COALESCE(block::text, SUBSTRING(bbl::text, 2, 5)) AS block,
        COALESCE(lot::text, SUBSTRING(bbl::text, 7, 4)) AS lot
    FROM dof_dtm
),

dof_dtm_tmp AS (
    SELECT
        bbl,
        boro,
        block,
        lot,
        ROW_NUMBER() OVER () AS id,
        ST_MULTI(ST_UNION(ST_MAKEVALID(geom))) AS geom
    FROM coalesced
    GROUP BY bbl, boro, block, lot
)

SELECT * FROM dof_dtm_tmp
