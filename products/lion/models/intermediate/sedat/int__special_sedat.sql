{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['b7sc']},
    ]
) }}

WITH special_sedat AS (
    SELECT * FROM {{ source("recipe_sources", "dcp_cscl_specialsedat") }}
),

street_names AS (
    SELECT
        b7sc,
        lookup_key AS street_name
    FROM {{ source("recipe_sources", "dcp_cscl_streetname") }}
    WHERE principal_flag = 'Y'
),

feature_names AS (
    SELECT
        b7sc,
        lookup_key AS street_name
    FROM {{ source("recipe_sources", "dcp_cscl_featurename") }}
    WHERE principal_flag = 'Y'
)

SELECT
    ss.lionkey,
    ss.parity,
    COALESCE(sn.street_name, fn.street_name) AS street_name,
    CASE
        WHEN ss.side = '1' THEN 'L'
        WHEN ss.side = '2' THEN 'R'
    END AS side_of_street,
    ss.lowaddress,
    ss.low_addr_suffix,
    ss.highaddress,
    ss.high_addr_suffix,
    ss.election_district,
    ss.assembly_district,
    ss.b7sc,
    COALESCE(COALESCE(sn.street_name, fn.street_name) IS NULL, FALSE) AS missing_street_name
FROM special_sedat AS ss
LEFT JOIN street_names AS sn ON ss.b7sc = sn.b7sc
LEFT JOIN feature_names AS fn ON ss.b7sc = fn.b7sc
