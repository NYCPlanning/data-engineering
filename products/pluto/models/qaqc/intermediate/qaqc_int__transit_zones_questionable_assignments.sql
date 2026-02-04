{{ config(
    materialized = 'view'
) }}
-- Analysis: Transit Zone Questionable Assignments
-- Purpose: Identify tax lots with multiple transit zone assignments and analyze coverage patterns

with winners_losers as (
select
    max(100 - abs(50 - rk.pct_covered )) over (partition by rk.bb) as risk
    ,rk.bb, rk.borough, rk.block
	,rk.transit_zone as winner_tz
	,rk2.transit_zone as loser_tz
	,rk.pct_covered as winner_pct
	,rk2.pct_covered as loser_pct
	,tb.geom as block_geom
	,(select ARRAY_AGG(DISTINCT zonedist1) from pluto p where p.borough = rk.borough and p.block = rk.block ) as block_zone_dists
	,ST_Envelope(st_buffer(tb.geom, .005)) as area_of_interest_geom
from transit_zones_block_to_tz_ranked rk
join transit_zones_block_to_tz_ranked as rk2 on rk.bb = rk2.bb and rk2."row_number" = 2
join transit_zones_tax_blocks_decomp_temp tb on tb.bb = rk.bb
where rk.bb in (select distinct bb from transit_zones_block_to_tz_ranked where row_number = 2) and rk."row_number" = 1
order by risk desc
)
select
	wl.block_geom
	,wl.risk
	,wl.winner_pct
	,wl.loser_pct
	,wl.bb
	,wl.borough
	,wl.block
	,wl.block_zone_dists
	,wl.winner_tz
	,wl.loser_tz
	,(select st_intersection(area_of_interest_geom, wkb_geometry) from dcp_transit_zones dtz where dtz.transit_zone = wl.winner_tz) as winner_tz_geom
	,(select st_intersection(area_of_interest_geom, wkb_geometry) from dcp_transit_zones dtz where dtz.transit_zone = wl.loser_tz) as loser_tz_geom
from winners_losers wl
where block_zone_dists != '{"PARK"}' and bb in ('BK-5802-1', 'BX-4058-1', 'BX-4062-1', 'BX-4091-1')
