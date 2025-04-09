{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
    ]
) }}
WITH centerline_offsets AS (
    SELECT * FROM {{ ref("int__centerline_offsets") }}
)

SELECT
    co.segmentid,
    left_poly.atomicid AS left_atomicid,
    left_poly.censustract_2000 AS left_2000_census_tract,
    left_poly.censustract_2010 AS left_2010_census_tract,
    left_poly.censustract_2020 AS left_2020_census_tract,
    left_poly.assemdist AS left_assembly_district,
    left_poly.electdist AS left_election_district,
    left_poly.schooldist AS left_school_district,
    right_poly.atomicid AS right_atomicid,
    right_poly.censustract_2000 AS right_2000_census_tract,
    right_poly.censustract_2010 AS right_2010_census_tract,
    right_poly.censustract_2020 AS right_2020_census_tract,
    right_poly.assemdist AS right_assembly_district,
    right_poly.electdist AS right_election_district,
    right_poly.schooldist AS right_school_district
FROM centerline_offsets AS co
-- using a cte around atomicpolygons confused the postgres compiler to not use index
LEFT JOIN
    {{ source("recipe_sources", "dcp_cscl_atomicpolygons") }} AS left_poly
    ON ST_WITHIN(co.left_offset_point, left_poly.geom)
LEFT JOIN
    {{ source("recipe_sources", "dcp_cscl_atomicpolygons") }} AS right_poly
    ON ST_WITHIN(co.left_offset_point, right_poly.geom)
