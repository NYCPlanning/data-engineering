with pluto as (
  select
    bbl,
    wkb_geometry
  from {{ source("recipe_sources", "dcp_mappluto_clipped") }}
),

block_groups as (
  select
    geoid,
    geom
  from {{ ref("stg__census_block_groups") }}
),

lot_block_group_intersections as (
  select
    pluto.bbl as bbl,
    pluto.wkb_geometry as lot_geometry,
    ST_AREA(pluto.wkb_geometry) as lot_area_sqft,
    block_groups.geoid as block_group_geoid,
    block_groups.geom as block_group_geometry
  from pluto
    left join block_groups
      on ST_INTERSECTS(pluto.wkb_geometry, block_groups.geom)
),

intersection_calculations as (
  select
    bbl,
    lot_geometry,
    lot_area_sqft,
    block_group_geoid,
    block_group_geometry,
    ST_AREA(
        CASE
            WHEN ST_COVEREDBY(lot_geometry, block_group_geometry) THEN lot_geometry
            ELSE ST_INTERSECTION(lot_geometry, block_group_geometry)
        END
    ) AS area_of_intersection_sqft
  from lot_block_group_intersections
),

intersection_ratios as (
  select
    bbl,
    block_group_geoid,
    area_of_intersection_sqft / lot_area_sqft as overlap_ratio
  from intersection_calculations
)

select * from intersection_ratios
