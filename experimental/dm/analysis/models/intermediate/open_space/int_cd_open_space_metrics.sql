-- residents by their street segment's walk time to open space, in Parks' segment file
WITH segment_residents AS (
    SELECT
        borocd,
        sum(population) AS open_space_residents,
        sum(population) FILTER (WHERE walk_minutes_to_open_space <= 10) AS open_space_residents_within_10_minutes
    FROM {{ ref('int_open_space_walk_segment_spine') }}
    GROUP BY borocd
),

-- 2020 residents of blocks with a point inside them in Parks' Walk to a Park service area. Each block counts whole,
-- served or not.
served_blocks AS (
    SELECT DISTINCT block.census_block_2020
    FROM {{ ref('stg_dcp__cb2020') }} AS block
    INNER JOIN {{ ref('int_walk_to_a_park_spine') }} AS served
        ON ST_Intersects(served.geom, ST_PointOnSurface(block.geom))
),

walk_to_a_park AS (
    SELECT
        block.borocd,
        sum(block.population_2020) AS walk_to_a_park_residents,
        coalesce(sum(block.population_2020) FILTER (WHERE served.census_block_2020 IS NOT NULL), 0)
            AS walk_to_a_park_residents_served
    FROM {{ ref('int_census_block_spine') }} AS block
    LEFT JOIN served_blocks AS served ON block.census_block_2020 = served.census_block_2020
    GROUP BY block.borocd
),

-- Vital Parks names districts like "Bronx 01"
vital_parks AS (
    SELECT
        spine.borocd,
        district.active_recreation_areas_pct,
        district.new_or_renovated_parks_pct,
        district.new_or_renovated_parks_future_pct,
        city.active_recreation_areas_pct AS citywide_active_recreation_areas_pct,
        city.new_or_renovated_parks_pct AS citywide_new_or_renovated_parks_pct,
        city.new_or_renovated_parks_future_pct AS citywide_new_or_renovated_parks_future_pct
    FROM {{ ref('int_cd_spine') }} AS spine
    LEFT JOIN {{ ref('stg_dpr__vital_parks_area_stats') }} AS district
        ON
            district.area_type = 'Community District'
            AND district.area_name = spine.boro_name || ' ' || lpad(spine.cd_number::VARCHAR, 2, '0')
    LEFT JOIN {{ ref('stg_dpr__vital_parks_area_stats') }} AS city ON city.area_type = 'Citywide'
),

-- parks with a project in any phase but completed, by the park IDs on Parks' Capital Project Tracker locations
parks_with_current_project AS (
    SELECT DISTINCT location.park_id
    FROM {{ ref('int_park_capital_project_location_spine') }} AS location
    INNER JOIN {{ ref('int_park_capital_project_spine') }} AS project ON location.tracker_id = project.tracker_id
    WHERE project.current_phase <> 'completed'
),

-- each park counts for the district nearest its interior point, 0 feet away when the point is inside. Beaches and
-- other shoreline parks have their point past the shoreline the districts stop at, like ferry landings.
districts_2263 AS (
    SELECT borocd, ST_Transform(geom, 'EPSG:4326', 'EPSG:2263', true) AS geom
    FROM {{ ref('int_cd_spine') }}
),

park_districts AS (
    SELECT
        park.park_id,
        any_value(park.acres) AS acres,
        arg_min(
            district.borocd,
            ST_Distance(district.geom, ST_Transform(ST_PointOnSurface(park.geom), 'EPSG:4326', 'EPSG:2263', true))
        ) AS borocd
    FROM {{ ref('int_park_spine') }} AS park
    CROSS JOIN districts_2263 AS district
    GROUP BY park.park_id
),

park_projects AS (
    SELECT
        park.borocd,
        count(*) AS parks,
        sum(park.acres) AS park_acres,
        count(*) FILTER (WHERE current.park_id IS NOT NULL) AS parks_with_current_project,
        coalesce(sum(park.acres) FILTER (WHERE current.park_id IS NOT NULL), 0) AS park_acres_with_current_project
    FROM park_districts AS park
    LEFT JOIN parks_with_current_project AS current ON park.park_id = current.park_id
    GROUP BY park.borocd
)

SELECT
    walk_to_a_park.borocd,
    segment_residents.open_space_residents,
    segment_residents.open_space_residents_within_10_minutes,
    walk_to_a_park.walk_to_a_park_residents,
    walk_to_a_park.walk_to_a_park_residents_served,
    vital_parks.active_recreation_areas_pct,
    vital_parks.new_or_renovated_parks_pct,
    vital_parks.new_or_renovated_parks_future_pct,
    vital_parks.citywide_active_recreation_areas_pct,
    vital_parks.citywide_new_or_renovated_parks_pct,
    vital_parks.citywide_new_or_renovated_parks_future_pct,
    coalesce(park_projects.parks, 0) AS parks,
    coalesce(park_projects.park_acres, 0) AS park_acres,
    coalesce(park_projects.parks_with_current_project, 0) AS parks_with_current_project,
    coalesce(park_projects.park_acres_with_current_project, 0) AS park_acres_with_current_project
FROM walk_to_a_park
LEFT JOIN segment_residents ON walk_to_a_park.borocd = segment_residents.borocd
LEFT JOIN vital_parks ON walk_to_a_park.borocd = vital_parks.borocd
LEFT JOIN park_projects ON walk_to_a_park.borocd = park_projects.borocd
