with lot_block_groups as (
  select * from {{ ref("int__lot_block_groups_details") }}
),

block_groups_income as (
  select * from {{ ref("stg__low_mod_by_block_group") }}
),

block_groups_floor_area as (
  select
    block_group_geoid as geoid,
    sum(bldgarea_in_block_group) as total_floor_area,
    sum(resarea_in_block_group) as residential_floor_area
  from lot_block_groups
  group by geoid
),

block_group_details as (
  select
    block_groups_floor_area.geoid,
    block_groups_income.boro as borough_name,
    block_groups_income.tract,
    block_groups_income.block_group,
    total_floor_area,
    residential_floor_area,
    case
      when total_floor_area = 0
        then 0
      else (residential_floor_area / total_floor_area) * 100
    end as residential_floor_area_percentage,
    block_groups_income.total_population,
    block_groups_income.lowmod_population as low_mod_income_population,
    block_groups_income.lowmod_pct as low_mod_income_population_percentage
  from block_groups_floor_area
    left join block_groups_income
    on block_groups_floor_area.geoid = block_groups_income.geoid
)

select * from block_group_details