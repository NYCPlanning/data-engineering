{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['globalid']},
    ]
) }}
-- Left/right AtomicPolygons and their attributes per segment. Two answers are computed,
-- each in its own pair of columns: probe_* (points 2 ft either side of the midpoint,
-- int__segment_offsets) and topo_* (the AP topology edges the segment runs along,
-- int__segment_topology_sides). lion_lr_from_topology picks which pair fills
-- left_atomicid/right_atomicid and every left_*/right_* attribute (see dbt_project.yml).

{% if var('lion_lr_from_topology') %}
    {% set chosen, chosen_prefix = 'ts', 'topo' %}
{% else %}
    {% set chosen, chosen_prefix = 'p', 'probe' %}
{% endif %}

WITH segment_offsets AS (
    SELECT * FROM {{ ref("int__segment_offsets") }}
),

probe AS (
    SELECT
        so.globalid,
        so.lionkey,
        so.segmentid,
        so.boroughcode,
        left_poly.atomicid AS probe_left_atomicid,
        right_poly.atomicid AS probe_right_atomicid
    FROM segment_offsets AS so
    LEFT JOIN
        {{ ref("stg__atomicpolygons") }} AS left_poly
        ON st_within(so.left_offset_point, left_poly.geom)
    LEFT JOIN
        {{ ref("stg__atomicpolygons") }} AS right_poly
        ON st_within(so.right_offset_point, right_poly.geom)
),

chosen AS (
    SELECT
        p.*,
        ts.topo_left_atomicid,
        ts.topo_right_atomicid,
        ts.method AS topo_method,
        {{ chosen }}.{{ chosen_prefix }}_left_atomicid AS left_atomicid,
        {{ chosen }}.{{ chosen_prefix }}_right_atomicid AS right_atomicid
    FROM probe AS p
    LEFT JOIN {{ ref("int__segment_topology_sides") }} AS ts ON p.globalid = ts.globalid
)

SELECT
    so.globalid,
    so.lionkey,
    so.segmentid,
    so.boroughcode AS segment_borocode,
    left_poly.atomicid AS left_atomicid,
    left_poly.borocode AS left_borocode,
    left_poly.commdist AS left_community_district,
    left_poly.censustract_1990_basic AS left_1990_census_tract_basic,
    left_poly.censustract_1990_suffix AS left_1990_census_tract_suffix,
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
    right_poly.commdist AS right_community_district,
    right_poly.censustract_1990_basic AS right_1990_census_tract_basic,
    right_poly.censustract_1990_suffix AS right_1990_census_tract_suffix,
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
    right_poly.schooldist AS right_school_district,
    so.probe_left_atomicid,
    so.probe_right_atomicid,
    so.topo_left_atomicid,
    so.topo_right_atomicid,
    so.topo_method
FROM chosen AS so
LEFT JOIN {{ ref("stg__atomicpolygons") }} AS left_poly ON so.left_atomicid = left_poly.atomicid
LEFT JOIN {{ ref("stg__atomicpolygons") }} AS right_poly ON so.right_atomicid = right_poly.atomicid
