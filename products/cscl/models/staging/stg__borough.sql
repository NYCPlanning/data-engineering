{{ config(
    materialized = 'table'
) }}

SELECT
    globalid,
    borocode,
    boroname,
    county,
    fips,
    st_makevalid(linearize(geom)) AS geom,
    geom AS raw_geom
FROM {{ source("recipe_sources", "dcp_cscl_borough") }}
