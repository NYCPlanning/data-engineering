with block_groups as (
  select
    *,
    left(geoid, -1) as tract_id
  from {{ ref("int__block_groups") }}
),

tracts as (
  select
    tract_id as geoid,
    max(borough_name) as borough_name,
    sum(total_floor_area) as total_floor_area,
    sum(residential_floor_area) as residential_floor_area,
    sum(total_population) as total_population,
    sum(low_mod_income_population) as low_mod_income_population
  from block_groups
  group by tract_id
),

tracts_calculation as (
  select
    *,
    case
      when total_floor_area = 0
        then 0
      else (residential_floor_area / total_floor_area) * 100
    end as residential_floor_area_percentage,
    case
      when total_population = 0
        then 0
      else (low_mod_income_population / total_population) * 100
    end as low_mod_income_population_percentage
  from tracts
)

select * from tracts_calculation