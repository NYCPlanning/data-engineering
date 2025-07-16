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

rezone_bbl AS (
    SELECT
        a.bbl,
        a.dtm_id,
        '1' AS notes
    FROM dtm AS a
    INNER JOIN dcp_zoningmapamendments AS b
        ON ST_Intersects(a.geom, b.geom)
    WHERE
        b.project_na = 'Inwood Rezoning'
        AND a.bbl != '1022552000'
        AND a.bbl != '1022550001'
        AND a.bbl != '1021890001'
        AND a.bbl != '1021970001'
)

SELECT * FROM rezone_bbl
