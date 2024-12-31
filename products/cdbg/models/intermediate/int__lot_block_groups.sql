with lot_block_groups as (
  select
    bbl,
    block_group_geoid,
    overlap_ratio
  from {{ ref("int__lot_block_groups_raw") }}
),

valid_lot_block_groups as (
  select * from lot_block_groups
  where overlap_ratio is not null
),

lots_easy as (
  select
    bbl,
    block_group_geoid,
    1 as overlap_ratio
  from valid_lot_block_groups
    where overlap_ratio > 0.9
),

lots_split as (
  select
    *
  from valid_lot_block_groups
    where bbl not in (select bbl from lots_easy)
),

lots as (
  select * from lots_easy
  union all
  select * from lots_split
)

select * from lots
