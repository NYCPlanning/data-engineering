WITH dof_dtm AS (
    SELECT * FROM {{ ref('stg__dof_dtm') }}
),

dcp_zoningmapamendments AS (
    SELECT * FROM {{ ref('stg__dcp_zoningmapamendments') }}
),

rezone_bbl AS (
    SELECT
        b.bbl,
        '1' AS notes
    FROM dof_dtm AS b, dcp_zoningmapamendments AS c
    WHERE
        b.geom IS NOT null
        AND c.project_na = 'Inwood Rezoning'
        AND ST_INTERSECTS(b.geom, c.geom)
)

-- when joining this to ztl product table, include the conditions that
-- AND a.bbl != '1022552000'
-- AND a.bbl != '1022550001'
-- AND a.bbl != '1021890001'
-- AND a.bbl != '1021970001'
-- a being the product table 
-- left join to the ztl product table and set null to 0 with coalesce

SELECT * FROM rezone_bbl
