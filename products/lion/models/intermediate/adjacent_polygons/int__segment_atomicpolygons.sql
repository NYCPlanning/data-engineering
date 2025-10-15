{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
    ]
) }}
WITH segment_offsets AS (
    SELECT * FROM {{ ref("int__segment_offsets") }}
)

SELECT
    so.lionkey_dev,
    so.segmentid,
    so.boroughcode AS segment_borocode,
    left_poly.atomicid AS left_atomicid,
    left_poly.borocode AS left_borocode,
    left_poly.censustract_2000_basic AS left_2000_census_tract_basic,
    left_poly.censustract_2000_suffix AS left_2000_census_tract_suffix,
    left_poly.censustract_2000 AS left_2000_census_tract,
    left_poly.censustract_2010_basic AS left_2010_census_tract_basic,
    left_poly.censustract_2010_suffix AS left_2010_census_tract_suffix,
    left_poly.censustract_2010 AS left_2010_census_tract,
    left_poly.censustract_2020_basic AS left_2020_census_tract_basic,
    left_poly.censustract_2020_suffix AS left_2020_census_tract_suffix,
    left_poly.censustract_2020 AS left_2020_census_tract,
    left_poly.censusblock_2000_basic AS left_2000_census_block_basic,
    left_poly.censusblock_2000_suffix AS left_2000_census_block_suffix,
    left_poly.censusblock_2010_basic AS left_2010_census_block_basic,
    left_poly.censusblock_2010_suffix AS left_2010_census_block_suffix,
    left_poly.censusblock_2020_basic AS left_2020_census_block_basic,
    left_poly.censusblock_2020_suffix AS left_2020_census_block_suffix,
    left_poly.assemdist AS left_assembly_district,
    left_poly.electdist AS left_election_district,
    left_poly.schooldist AS left_school_district,
    right_poly.atomicid AS right_atomicid,
    right_poly.borocode AS right_borocode,
    right_poly.censustract_2000_basic AS right_2000_census_tract_basic,
    right_poly.censustract_2000_suffix AS right_2000_census_tract_suffix,
    right_poly.censustract_2000 AS right_2000_census_tract,
    right_poly.censustract_2010_basic AS right_2010_census_tract_basic,
    right_poly.censustract_2010_suffix AS right_2010_census_tract_suffix,
    right_poly.censustract_2010 AS right_2010_census_tract,
    right_poly.censustract_2020_basic AS right_2020_census_tract_basic,
    right_poly.censustract_2020_suffix AS right_2020_census_tract_suffix,
    right_poly.censustract_2020 AS right_2020_census_tract,
    right_poly.censusblock_2000_basic AS right_2000_census_block_basic,
    right_poly.censusblock_2000_suffix AS right_2000_census_block_suffix,
    right_poly.censusblock_2010_basic AS right_2010_census_block_basic,
    right_poly.censusblock_2010_suffix AS right_2010_census_block_suffix,
    right_poly.censusblock_2020_basic AS right_2020_census_block_basic,
    right_poly.censusblock_2020_suffix AS right_2020_census_block_suffix,
    right_poly.assemdist AS right_assembly_district,
    right_poly.electdist AS right_election_district,
    right_poly.schooldist AS right_school_district
FROM segment_offsets AS so
-- using a cte around atomicpolygons confused the postgres compiler to not use index
LEFT JOIN
    {{ ref("stg__atomicpolygons") }} AS left_poly
    ON st_within(so.left_offset_point, left_poly.geom)
LEFT JOIN
    {{ ref("stg__atomicpolygons") }} AS right_poly
    ON st_within(so.right_offset_point, right_poly.geom)
