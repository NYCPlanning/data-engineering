-- one row per district and project with a location in the district. A project's point here is its location in the
-- district nearest the center of all its locations there, measured in feet, so it's a real place such as a station.
-- Ties go to the lowest location number.
WITH located AS (
    SELECT
        district.borocd,
        location.*,
        ST_Transform(location.geom, 'EPSG:4326', 'EPSG:2263', true) AS geom_2263
    FROM {{ ref('int_mta_capital_project_location_spine') }} AS location
    INNER JOIN {{ ref('int_cd_spine') }} AS district ON ST_Intersects(district.geom, location.geom)
),

centers AS (
    SELECT
        borocd,
        acep,
        count(*) AS locations_in_district,
        ST_Centroid(ST_Collect(list(geom_2263))) AS center
    FROM located
    GROUP BY borocd, acep
),

ranked AS (
    SELECT
        located.borocd,
        located.acep,
        located.plan_series,
        located.capital_plan,
        located.agency_name,
        located.category,
        located.element,
        located.project_description,
        centers.locations_in_district,
        located.location_number AS representative_location_number,
        located.geom,
        row_number() OVER (
            PARTITION BY located.borocd, located.acep
            ORDER BY ST_Distance(located.geom_2263, centers.center), located.location_number
        ) AS distance_rank
    FROM located
    INNER JOIN centers ON located.borocd = centers.borocd AND located.acep = centers.acep
)

SELECT * EXCLUDE (distance_rank)
FROM ranked
WHERE distance_rank = 1
