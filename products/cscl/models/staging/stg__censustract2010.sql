{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

SELECT
    ctlabel,
    borocode,
    neighborhood_code,
    ct,
    borocode || ct AS boroct, -- BUG! There's a boroct field, but has a few incorrect values, causing dupes
    cd_eligibility,
    puma,
    empowerment_zone,
    mcea,
    created_by,
    created_date,
    modified_by,
    modified_date,
    health_area,
    globalid,
    shape_length,
    shape_area,
    geom AS raw_geom,
    LINEARIZE(geom) AS geom
FROM {{ source("recipe_sources", "dcp_cscl_censustract2010") }}
