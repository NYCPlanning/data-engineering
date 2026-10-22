{{ config(
    materialized = 'table'
) }}

SELECT
    globalid,
    zip_code,
    po_name,
    county,
    created_by,
    created_date,
    modified_by,
    modified_date,
    st_makevalid(geom) AS geom,
    geom AS raw_geom
FROM {{ source("recipe_sources", "dcp_cscl_zipcode") }}
