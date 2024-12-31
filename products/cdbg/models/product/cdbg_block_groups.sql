with block_groups as (
  select * from {{ ref("int__block_groups") }}
),

eligibility_calculation as (
  select
    *,
    low_mod_income_population_percentage > 51 and residential_floor_area_percentage > 50 as eligibility_flag
  from block_groups
),

eligibility as (
  select
    *,
    case
      when eligibility_flag then 'CD Eligible'
      else 'Ineligible'
    end as eligibility
  from eligibility_calculation
)

select * from eligibility