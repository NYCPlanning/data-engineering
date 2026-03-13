{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

SELECT
    atomicid,
    borough AS borocode,
    -- census 1990
    censustract_1990,
    left(censustract_1990, 4)::INT AS censustract_1990_basic,
    nullif(right(censustract_1990, 2), '00')::INT AS censustract_1990_suffix,
    -- census 2000
    censustract_2000,
    left(censustract_2000, 4)::INT AS censustract_2000_basic,
    censusblock_2000::INT AS censusblock_2000_basic,
    censusblock_2000_suffix,
    nullif(right(censustract_2000, 2), '00')::INT AS censustract_2000_suffix,
    -- census 2010
    censustract_2010,
    left(censustract_2010, 4)::INT AS censustract_2010_basic,
    nullif(right(censustract_2010, 2), '00')::INT AS censustract_2010_suffix,
    censusblock_2010 AS censusblock_2010_raw,
    censusblock_2010::INT AS censusblock_2010_basic,
    censusblock_2010_suffix::INT,
    CASE
        WHEN RIGHT(censustract_2010, 2) = '00' THEN ''
        ELSE LTRIM(RIGHT(censustract_2010, 2), '0')
    END AS census_tract_2010_suffix_test,
    -- census 2020
    censustract_2020,
    left(censustract_2020, 4)::INT AS censustract_2020_basic,
    nullif(right(censustract_2020, 2), '00')::INT AS censustract_2020_suffix,
    censusblock_2020::INT AS censusblock_2020_basic,
    censusblock_2020_suffix::INT,
    --
    RIGHT(atomicid, 3) AS dynamic_block,
    nullif(assemdist, ' ') AS assemdist,
    nullif(electdist, ' ') AS electdist,
    nullif(schooldist, '0') AS schooldist,
    commdist,
    LEFT(admin_fire_company, 1) AS fire_company_type,
    RIGHT(admin_fire_company, 3) AS fire_company_number,
    sb1_volume,
    sb1_page,
    sb2_volume,
    sb2_page,
    sb3_volume,
    sb3_page,
    water_flag,
    commercial_waste_zone,
    hurricane_evacuation_zone,
    st_makevalid(linearize(geom)) AS geom,
    geom AS raw_geom
FROM {{ source("recipe_sources", "dcp_cscl_atomicpolygons") }}
