{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

SELECT
    globalid,
    name,
    lp_number,
    designated,
    extension,
    borough,
    created_by,
    created_date,
    modified_by,
    modified_date,
    st_makevalid(linearize(geom)) AS geom,
    geom AS raw_geom
FROM {{ source("recipe_sources", "dcp_cscl_historicdistrict") }}
