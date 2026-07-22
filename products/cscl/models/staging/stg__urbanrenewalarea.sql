{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

SELECT
    globalid,
    uranum,
    uraid,
    boro_name,
    boro_num,
    site_name,
    urp,
    st_makevalid(linearize(geom)) AS geom,
    geom AS raw_geom
FROM {{ source("recipe_sources", "dcp_cscl_urbanrenewalarea") }}
