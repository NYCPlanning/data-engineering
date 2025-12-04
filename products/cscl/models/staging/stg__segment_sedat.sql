SELECT
    segmentid,
    lionkey,
    side
FROM {{ source("recipe_sources", "dcp_cscl_sedat") }}
UNION ALL
SELECT
    segmentid,
    lionkey,
    side
FROM {{ source("recipe_sources", "dcp_cscl_specialsedat") }}
