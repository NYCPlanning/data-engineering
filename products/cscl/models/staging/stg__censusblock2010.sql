{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

SELECT
    globalid,
    borocode,
    ct,
    cb,
    cb_suffix,
    bctcb,
    st_makevalid(linearize(geom)) AS geom,
    geom AS raw_geom
FROM {{ source("recipe_sources", "dcp_cscl_censusblock2010") }}
