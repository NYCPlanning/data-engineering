{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

SELECT
    atomicid,
    borough AS borocode,
    censustract_2000,
    left(censustract_2000, 4)::int AS censustract_2000_basic,
    nullif(right(censustract_2000, 2), '00')::int AS censustract_2000_suffix,
    censustract_2010,
    left(censustract_2010, 4)::int AS censustract_2010_basic,
    nullif(right(censustract_2010, 2), '00')::int AS censustract_2010_suffix,
    censustract_2020,
    left(censustract_2020, 4)::int AS censustract_2020_basic,
    nullif(right(censustract_2020, 2), '00')::int AS censustract_2020_suffix,
    censusblock_2000::int AS censusblock_2000_basic,
    censusblock_2000_suffix,
    censusblock_2010::int AS censusblock_2010_basic,
    censusblock_2010_suffix::int,
    censusblock_2020::int AS censusblock_2020_basic,
    censusblock_2020_suffix::int,
    assemdist,
    electdist,
    nullif(schooldist, '0') AS schooldist,
    geom
FROM {{ source("recipe_sources", "dcp_cscl_atomicpolygons") }}
