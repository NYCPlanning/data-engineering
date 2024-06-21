{{ config(
    materialized = 'table',
    indexes=[
        {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

WITH dof_dtm AS (
    SELECT * FROM {{ source('recipe_sources', 'dof_dtm') }}
),

rename AS(
    SELECT 
        ogc_fid,
        bbl,
        wkb_geometry as geom,
        boro,
        block,
        lot
    FROM dof_dtm
),

coalesced AS (
    SELECT
        bbl,
        COALESCE(boro::text, LEFT(bbl::text, 1)) AS boro,
        COALESCE(block::text, SUBSTRING(bbl::text, 2, 5)) AS block,
        COALESCE(lot::text, SUBSTRING(bbl::text, 7, 4)) AS lot,
        geom
    FROM rename
    ),

dof_dtm_tmp AS(
    SELECT
        ROW_NUMBER() OVER () AS id,
        bbl,
        boro,
        block,
        lot,
        ST_MULTI(ST_UNION(ST_MAKEVALID(geom))) AS geom
    FROM coalesced
    GROUP BY bbl, boro, block, lot
)

SELECT * FROM dof_dtm_tmp
