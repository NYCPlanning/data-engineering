WITH validdtm AS (
    SELECT * FROM {{ ref('int__validdtm') }}
),

dcp_zoningmapamendments AS (
    SELECT * FROM {{ ref('stg__dcp_zoningmapamendments') }}
),

inzonechange AS (
    SELECT
        a.dtm_id,
        'Y' AS inzonechange
    FROM validdtm AS a
    INNER JOIN dcp_zoningmapamendments AS b
        ON
            ST_INTERSECTS(a.geom, b.geom)
            AND b.effective::DATE > CURRENT_DATE - INTERVAL '2 months'
)
-- left join to the product ztl table 
-- use coalesce to set null to 0 

SELECT * FROM inzonechange
