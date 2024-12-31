with lot_block_groups as (
  select * from {{ ref("int__lot_block_groups") }}
),

pluto as (
  select
    bbl,
    bldgarea,
    resarea
  from {{ source("recipe_sources", "dcp_mappluto_clipped") }}
),

details as (
  select
    pluto.bbl,
    lot_block_groups.block_group_geoid,
    pluto.bldgarea,
    pluto.resarea,
    lot_block_groups.overlap_ratio
  from lot_block_groups
    left join pluto
      on lot_block_groups.bbl = pluto.bbl
),

ratio_details as (
  select
    bbl,
    block_group_geoid,
    overlap_ratio,
    bldgarea,
    bldgarea * overlap_ratio as bldgarea_in_block_group,
    resarea,
    resarea * overlap_ratio as resarea_in_block_group
  from details
)

select * from ratio_details
