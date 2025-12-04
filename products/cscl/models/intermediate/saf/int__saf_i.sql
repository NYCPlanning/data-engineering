WITH intersection AS (
    SELECT * FROM {{ source('recipe_sources','dcp_cscl_namedintersection') }}
),
feature_names AS (
    SELECT * FROM {{ ref('stg__facecode_and_featurename') }}
)
SELECT
    feature_names.place_name_sort_order AS place_name,
    intersection.nodeid,
    CASE WHEN intersection.multiplefield = 'Y' THEN 'M' END AS multiplefield,
    intersection.b7sc AS b7sc_intersection,
    intersection.b5sc_cross1,
    intersection.b5sc_cross2,
    'I' AS saftype
FROM intersection
LEFT JOIN feature_names ON intersection.b7sc = feature_names.b7sc
