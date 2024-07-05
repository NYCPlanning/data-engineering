WITH dof_dtm AS (
    SELECT * FROM {{ ref('stg__dof_dtm') }}
),

dcp_zoningmapamendments AS (
    SELECT * FROM {{ ref('stg__dcp_zoningmapamendments') }}
),

rezone_bbl AS (
    SELECT
        b.bbl,
        b.id AS dtm_id,
        '1' AS notes
    FROM dof_dtm AS b, dcp_zoningmapamendments AS c
    WHERE
        b.geom IS NOT null
        AND c.project_na = 'Inwood Rezoning'
        AND ST_INTERSECTS(b.geom, c.geom)
        AND b.bbl != '1022552000'
        AND b.bbl != '1022550001'
        AND b.bbl != '1021890001'
        AND b.bbl != '1021970001'
)

SELECT * FROM rezone_bbl
