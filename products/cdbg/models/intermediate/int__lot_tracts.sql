with pluto as (
  select
    bbl,
    wkb_geometry
  from {{ source("recipe_sources", "dcp_mappluto_clipped") }}
),

tracts as (
  select
    *
  from {{ source("recipe_sources", "dcp_ct2020_wi") }}
),

lot_tracts as (
  select
    pluto.bbl,
    pluto.wkb_geometry as lot_geometry,
    tracts.geoid,
    tracts.wkb_geometry as tract_geometry
  from pluto
    left join tracts
      on ST_Intersects(pluto.wkb_geometry, tracts.wkb_geometry)
)

select * from lot_tracts