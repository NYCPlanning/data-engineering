WITH dof_dtm AS (
    SELECT * FROM {{ ref('stg__dof_dtm') }}
),

dcp_zoningmapamendments AS (
    SELECT * FROM {{ ref('stg__dcp_zoningmapamendments') }}
),

inzonechange AS (
    SELECT 'Y' AS inzonechange
    FROM dof_dtm a
    INNER JOIN dcp_zoningmapamendments b
        ON ST_INTERSECTS(a.geom, b.geom)
        AND b.effective::DATE > CURRENT_DATE - INTERVAL '2 months'
)
-- left join to the product ztl table 
-- use coalesce to set null to 0 

SELECT * FROM inzonechange
