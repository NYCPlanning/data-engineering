-- A district's shares by drainage type should add up to at most its whole area. More would mean areas of
-- different types overlap. Less is expected, since the MS4 map leaves out combined and sanitary sewer areas.
SELECT
    borocd,
    sum(share_of_district) AS share_covered
FROM {{ ref('dim_community_district_ms4_drainage') }}
GROUP BY borocd
HAVING sum(share_of_district) > 1.01
