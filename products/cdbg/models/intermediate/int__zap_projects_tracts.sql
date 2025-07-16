WITH lots_to_tracts AS (
    SELECT * FROM {{ ref("int__zap_lot_tracts") }}
),

grouped_by_project AS (
    SELECT
        project_id,
        array_agg(
            bbl
            ORDER BY bbl ASC
        ) AS bbls,
        ST_Union(lot_geom) AS project_geom,
        bool_or(intersects_half_mile) AS partially_within_half_mile_of_eligible_tract,
        bool_or(intersects_quarter_mile) AS partially_within_quarter_mile_of_eligible_tract,
        ST_Union(tract_buffer_half_mile) AS eligibility_buffer_half_mile,
        ST_Union(tract_buffer_quarter_mile) AS eligibility_buffer_quarter_mile
    FROM lots_to_tracts
    GROUP BY project_id
)

SELECT
    project_id,
    bbls,
    project_geom,
    partially_within_half_mile_of_eligible_tract,
    partially_within_quarter_mile_of_eligible_tract,
    ST_Contains(eligibility_buffer_half_mile, project_geom) AS entirely_within_half_mile_of_eligible_tract,
    ST_Contains(eligibility_buffer_quarter_mile, project_geom) AS entirely_within_quarter_mile_of_eligible_tract,
    eligibility_buffer_half_mile,
    eligibility_buffer_quarter_mile
FROM grouped_by_project
