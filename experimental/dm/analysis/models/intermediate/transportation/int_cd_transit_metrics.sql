-- stations inside each district, once each (the MTA lists a station on two lines, such as 145 St, once per line,
-- under one station_id). A station on a district's edge counts for the one its point falls in.
WITH subway AS (
    SELECT
        district.borocd,
        count(DISTINCT station.station_id) AS subway_stations,
        count(DISTINCT station.station_id) FILTER (
            WHERE station.ada_accessibility = 'Full'
        ) AS subway_stations_fully_accessible,
        count(DISTINCT station.station_id) FILTER (
            WHERE station.ada_accessibility = 'Partial'
        ) AS subway_stations_partially_accessible
    FROM {{ ref('int_cd_spine') }} AS district
    INNER JOIN {{ ref('int_subway_station_spine') }} AS station ON ST_Intersects(district.geom, station.geom)
    GROUP BY district.borocd
),

ibx AS (
    SELECT
        district.borocd,
        count(*) AS proposed_ibx_stations
    FROM {{ ref('int_cd_spine') }} AS district
    INNER JOIN {{ ref('int_ibx_station_spine') }} AS station ON ST_Intersects(district.geom, station.geom)
    GROUP BY district.borocd
),

-- landings are on piers past the shoreline the districts stop at, so each counts for the district nearest it
ferry AS (
    SELECT
        borocd,
        count(*) AS ferry_landings
    FROM (
        SELECT
            landing.ferry_landing_id,
            arg_min(
                district.borocd,
                ST_Distance(
                    ST_Transform(district.geom, 'EPSG:4326', 'EPSG:2263', true),
                    ST_Transform(landing.geom, 'EPSG:4326', 'EPSG:2263', true)
                )
            ) AS borocd
        FROM {{ ref('int_ferry_landing_spine') }} AS landing
        CROSS JOIN {{ ref('int_cd_spine') }} AS district
        GROUP BY landing.ferry_landing_id
    )
    GROUP BY borocd
),

rail AS (
    SELECT
        district.borocd,
        count(*) AS rail_stations
    FROM {{ ref('int_cd_spine') }} AS district
    INNER JOIN {{ ref('int_rail_station_spine') }} AS station ON ST_Intersects(district.geom, station.geom)
    GROUP BY district.borocd
),

bus AS (
    SELECT
        district.borocd,
        count(*) AS bus_stops
    FROM {{ ref('int_cd_spine') }} AS district
    INNER JOIN {{ ref('int_bus_stop_spine') }} AS stop ON ST_Intersects(district.geom, stop.geom)
    GROUP BY district.borocd
),

-- 2020 residents by their block's transit access percentile. Percentiles are among the file's blocks, so the 50th is
-- the median block. Some populated blocks aren't in the file, and their residents are counted on their own.
access_residents AS (
    SELECT
        block.borocd,
        sum(block.population_2020) AS transit_access_residents,
        coalesce(sum(block.population_2020) FILTER (WHERE access.acres_reachable_percentile >= 50), 0)
            AS transit_access_residents_at_or_above_median_block,
        coalesce(sum(block.population_2020) FILTER (WHERE access.acres_reachable_percentile <= 10), 0)
            AS transit_access_residents_at_or_below_10th_percentile,
        coalesce(sum(block.population_2020) FILTER (WHERE access.census_block_2020 IS NULL), 0)
            AS transit_access_residents_not_in_file
    FROM {{ ref('int_census_block_spine') }} AS block
    LEFT JOIN {{ ref('int_transit_access_block_spine') }} AS access
        ON block.census_block_2020 = access.census_block_2020
    GROUP BY block.borocd
),

-- each block in the transit access file counts for the district a point inside it falls in, with its whole area,
-- for a median weighted by area. Block and district lines don't quite match, so cutting blocks at district lines
-- would leave slivers. The file leaves out some land, such as cemeteries and parks, so covered_share says how much
-- of the district its blocks cover.
access_pieces AS (
    SELECT
        district.borocd,
        block.acres_reachable_in_60_minutes,
        block.acres_reachable_percentile,
        ST_Area(ST_Transform(block.geom, 'EPSG:4326', 'EPSG:2263', true)) / 43560 AS piece_acres
    FROM {{ ref('int_cd_spine') }} AS district
    INNER JOIN {{ ref('int_transit_access_block_spine') }} AS block
        ON ST_Contains(district.geom, ST_PointOnSurface(block.geom))
),

access_running AS (
    SELECT
        *,
        sum(piece_acres) OVER (
            PARTITION BY borocd ORDER BY acres_reachable_in_60_minutes ROWS UNBOUNDED PRECEDING
        ) AS running_acres,
        sum(piece_acres) OVER (PARTITION BY borocd) AS covered_acres
    FROM access_pieces
    WHERE piece_acres > 0
),

access AS (
    SELECT
        access_running.borocd,
        min(access_running.acres_reachable_in_60_minutes) FILTER (
            WHERE access_running.running_acres >= access_running.covered_acres / 2
        ) AS median_acres_reachable_by_transit,
        -- the median is one block's value, so this is that block's percentile among the file's blocks
        min(access_running.acres_reachable_percentile) FILTER (
            WHERE access_running.running_acres >= access_running.covered_acres / 2
        ) AS median_acres_reachable_by_transit_percentile,
        any_value(access_running.covered_acres)
        / any_value(ST_Area(ST_Transform(district.geom, 'EPSG:4326', 'EPSG:2263', true)) / 43560)
            AS transit_access_covered_share
    FROM access_running
    INNER JOIN {{ ref('int_cd_spine') }} AS district ON access_running.borocd = district.borocd
    GROUP BY access_running.borocd
)

SELECT
    spine.borocd,
    coalesce(subway.subway_stations, 0) AS subway_stations,
    coalesce(subway.subway_stations_fully_accessible, 0) AS subway_stations_fully_accessible,
    coalesce(subway.subway_stations_partially_accessible, 0) AS subway_stations_partially_accessible,
    coalesce(rail.rail_stations, 0) AS rail_stations,
    coalesce(ibx.proposed_ibx_stations, 0) AS proposed_ibx_stations,
    coalesce(ferry.ferry_landings, 0) AS ferry_landings,
    coalesce(bus.bus_stops, 0) AS bus_stops,
    access.median_acres_reachable_by_transit,
    access.median_acres_reachable_by_transit_percentile,
    coalesce(access.transit_access_covered_share, 0) AS transit_access_covered_share,
    coalesce(access_residents.transit_access_residents, 0) AS transit_access_residents,
    coalesce(access_residents.transit_access_residents_at_or_above_median_block, 0)
        AS transit_access_residents_at_or_above_median_block,
    coalesce(access_residents.transit_access_residents_at_or_below_10th_percentile, 0)
        AS transit_access_residents_at_or_below_10th_percentile,
    coalesce(access_residents.transit_access_residents_not_in_file, 0) AS transit_access_residents_not_in_file
FROM {{ ref('int_cd_spine') }} AS spine
LEFT JOIN subway ON spine.borocd = subway.borocd
LEFT JOIN rail ON spine.borocd = rail.borocd
LEFT JOIN ibx ON spine.borocd = ibx.borocd
LEFT JOIN ferry ON spine.borocd = ferry.borocd
LEFT JOIN bus ON spine.borocd = bus.borocd
LEFT JOIN access ON spine.borocd = access.borocd
LEFT JOIN access_residents ON spine.borocd = access_residents.borocd
