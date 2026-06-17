{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

SELECT
    globalid,
    electdist,
    assembly_district,
    congress_district,
    state_sen_district,
    muni_court_district,
    city_council_district,
    st_makevalid(linearize(geom)) AS geom,
    geom AS raw_geom
FROM {{ source("recipe_sources", "dcp_cscl_electiondistrict") }}
