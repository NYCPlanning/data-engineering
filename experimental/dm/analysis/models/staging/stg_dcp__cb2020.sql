SELECT
    geoid AS census_block_2020,
    bctcb2020,
    wkb_geometry AS geom
FROM {{ source('sources', 'dcp_cb2020') }}
