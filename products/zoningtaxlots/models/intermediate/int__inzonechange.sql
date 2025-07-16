{{ config(
    materialized = 'table',
    indexes=[
        {'columns': ['dtm_id']},
    ]
) }}

WITH dtm AS (
    SELECT * FROM {{ ref('stg__dof_dtm') }}
),

dcp_zoningmapamendments AS (
    SELECT * FROM {{ ref('stg__dcp_zoningmapamendments') }}
),

inzonechange AS (
    SELECT
        a.dtm_id,
        'Y' AS inzonechange
    FROM dtm AS a
    INNER JOIN dcp_zoningmapamendments AS b
        ON
            ST_Intersects(a.geom, b.geom)
            AND b.effective::date > current_date - interval '2 months'
)
-- left join to the product ztl table 
-- use coalesce to set null to 0 

SELECT * FROM inzonechange
