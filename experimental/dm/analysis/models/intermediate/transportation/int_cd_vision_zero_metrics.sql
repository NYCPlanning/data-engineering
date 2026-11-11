-- Streets from different sources don't share exact lines: district lines often run down street centerlines, and
-- projects on wide boulevards are often drawn on a different carriageway than the priority corridor. So one reach is
-- used for both. A street counts for a district within reach_feet of it (a street along the edge counts for the
-- districts on both sides), and a priority street has a project when one is within reach_feet. Measured in
-- EPSG:2263 (feet).
{%- set reach_feet = 50 %}
WITH districts AS (
    SELECT
        borocd,
        ST_Buffer(ST_Transform(geom, 'EPSG:4326', 'EPSG:2263', true), {{ reach_feet }}) AS geom
    FROM {{ ref('int_cd_spine') }}
),

project_reach AS (
    SELECT
        ST_Union_Agg(ST_Buffer(ST_Transform(geom, 'EPSG:4326', 'EPSG:2263', true), {{ reach_feet }}))
            FILTER (WHERE project_type = 'Corridor') AS corridor_projects,
        ST_Union_Agg(ST_Buffer(ST_Transform(geom, 'EPSG:4326', 'EPSG:2263', true), {{ reach_feet }})) AS all_projects
    FROM {{ ref('int_street_improvement_project_spine') }}
),

corridor_pieces AS (
    SELECT
        districts.borocd,
        ST_Intersection(districts.geom, ST_Transform(corridor.geom, 'EPSG:4326', 'EPSG:2263', true)) AS geom
    FROM districts
    INNER JOIN {{ ref('int_vision_zero_priority_corridor_spine') }} AS corridor
        ON ST_Intersects(districts.geom, ST_Transform(corridor.geom, 'EPSG:4326', 'EPSG:2263', true))
),

corridors AS (
    SELECT
        corridor_pieces.borocd,
        sum(ST_Length(corridor_pieces.geom)) / 5280 AS priority_corridor_miles,
        sum(ST_Length(ST_Intersection(corridor_pieces.geom, project_reach.corridor_projects)))
        / 5280 AS priority_corridor_miles_with_project
    FROM corridor_pieces
    CROSS JOIN project_reach
    GROUP BY corridor_pieces.borocd
),

intersections AS (
    SELECT
        districts.borocd,
        count(*) AS priority_intersections,
        count(*) FILTER (
            WHERE ST_Intersects(project_reach.all_projects, ST_Transform(intersection.geom, 'EPSG:4326', 'EPSG:2263', true))
        ) AS priority_intersections_with_project
    FROM districts
    INNER JOIN {{ ref('int_vision_zero_priority_intersection_spine') }} AS intersection
        ON ST_Intersects(districts.geom, ST_Transform(intersection.geom, 'EPSG:4326', 'EPSG:2263', true))
    CROSS JOIN project_reach
    GROUP BY districts.borocd
),

projects AS (
    SELECT
        districts.borocd,
        count(*) AS street_improvement_projects_2009_2026,
        count(*) FILTER (WHERE project.sip_year >= 2021) AS street_improvement_projects_2021_2026
    FROM districts
    INNER JOIN {{ ref('int_street_improvement_project_spine') }} AS project
        ON ST_Intersects(districts.geom, ST_Transform(project.geom, 'EPSG:4326', 'EPSG:2263', true))
    GROUP BY districts.borocd
)

SELECT
    spine.borocd,
    coalesce(corridors.priority_corridor_miles, 0) AS priority_corridor_miles,
    coalesce(corridors.priority_corridor_miles_with_project, 0) AS priority_corridor_miles_with_project,
    coalesce(intersections.priority_intersections, 0) AS priority_intersections,
    coalesce(intersections.priority_intersections_with_project, 0) AS priority_intersections_with_project,
    coalesce(projects.street_improvement_projects_2009_2026, 0) AS street_improvement_projects_2009_2026,
    coalesce(projects.street_improvement_projects_2021_2026, 0) AS street_improvement_projects_2021_2026
FROM {{ ref('int_cd_spine') }} AS spine
LEFT JOIN corridors ON spine.borocd = corridors.borocd
LEFT JOIN intersections ON spine.borocd = intersections.borocd
LEFT JOIN projects ON spine.borocd = projects.borocd
