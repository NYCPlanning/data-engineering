WITH eligible_tracts AS (
    SELECT * FROM {{ ref('int__eligible_tracts') }}
),

lots AS (
    SELECT * FROM {{ ref('int__zap_lots') }}
),

all_tracts_quarter_mile AS (
    SELECT ST_UNION(eligible_tracts.quarter_mile_buffer_geom) AS geom
    FROM eligible_tracts
),

all_tracts_half_mile AS (
    SELECT ST_UNION(eligible_tracts.half_mile_buffer_geom) AS geom
    FROM eligible_tracts
),

projects AS (
    SELECT
        project_id,
        ST_UNION(geom) AS project_geom
    FROM lots
    GROUP BY project_id
),

quarter_mile AS (
    SELECT
        projects.project_id,
        projects.project_geom,
        COALESCE(all_tracts_quarter_mile.geom IS NOT NULL, FALSE) AS contained_quarter_mile
    FROM projects
    LEFT JOIN all_tracts_quarter_mile
        ON ST_CONTAINS(all_tracts_quarter_mile.geom, projects.project_geom)
),

quarter_and_half_mile AS (
    SELECT
        quarter_mile.*,
        COALESCE(all_tracts_half_mile.geom IS NOT NULL, FALSE) AS contained_half_mile
    FROM quarter_mile
    LEFT JOIN all_tracts_half_mile
        ON ST_CONTAINS(all_tracts_half_mile.geom, quarter_mile.project_geom)
)

SELECT * FROM quarter_and_half_mile
ORDER BY
    project_id ASC
