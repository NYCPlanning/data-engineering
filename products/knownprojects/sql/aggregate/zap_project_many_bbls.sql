{{ config(materialized='table', tags=['aggregate_general']) }}

/*
ZAP projects that span more than one BBL.

Used to decide which projects are matched to boundaries as polygons rather
than by centroid, since a project across many lots is not well represented by
a single point.
*/

WITH project_bbls AS (
    SELECT DISTINCT
        bbl,
        -- the first 9 characters are the project id the rest of KPDB uses
        substring(project_id FROM 1 FOR 9) AS record_id
    FROM {{ source('recipe_sources', 'dcp_projectbbls') }}
)

SELECT record_id
FROM project_bbls
GROUP BY record_id
HAVING count(*) > 1
