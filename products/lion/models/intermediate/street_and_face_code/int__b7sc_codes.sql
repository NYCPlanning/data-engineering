{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['b7sc']},
    ]
) }}
WITH streets AS (
    SELECT * FROM {{ source("recipe_sources", "dcp_cscl_streetname") }}
    WHERE principal_flag = 'Y' AND b7sc IS NOT NULL AND facecode IS NOT NULL
),
features AS (
    SELECT * FROM {{ source("recipe_sources", "dcp_cscl_featurename") }}
    WHERE principal_flag = 'Y' AND b7sc IS NOT NULL AND facecode IS NOT NULL
)
SELECT
    COALESCE(streets.b7sc, features.b7sc) AS b7sc,
    streets.facecode AS street_facecode,
    features.facecode AS feature_facecode
FROM streets
FULL OUTER JOIN features ON streets.b7sc = features.b7sc
