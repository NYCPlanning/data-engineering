{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

SELECT
    ogc_fid,
    id,
    agency,
    action_,
    message,
    sector,
    precinct,
    geo_type,
    globalid,
    esz_number,
    post,
    created_by,
    created_date,
    modified_by,
    modified_date,
    zone,
    id1,
    id2,
    patrol_borough,
    shape_length,
    shape_area,
    geom AS raw_geom,
    LINEARIZE(geom) AS geom
FROM {{ source("recipe_sources", "dcp_cscl_nypdbeat") }}
