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
        B7SC,
        LOOKUP_KEY as STREET_NAME
    FROM {{ source("recipe_sources", "dcp_cscl_streetname") }}
    WHERE PRINCIPAL_FLAG = 'Y'
),

feature_names AS (
    SELECT 
        B7SC,
        LOOKUP_KEY as STREET_NAME
    FROM {{ source("recipe_sources", "dcp_cscl_featurename") }}
    WHERE PRINCIPAL_FLAG = 'Y'
)

SELECT 
    ss.LIONKEY,
    ss.PARITY,
    COALESCE(sn.STREET_NAME, fn.STREET_NAME) as STREET_NAME,
    CASE 
        WHEN ss.SIDE = '1' THEN 'L'
        WHEN ss.SIDE = '2' THEN 'R'
    END as SIDE_OF_STREET,
    ss.LOWADDRESS,
    ss.LOW_ADDR_SUFFIX,
    ss.HIGHADDRESS,
    ss.HIGH_ADDR_SUFFIX,
    ss.ELECTION_DISTRICT,
    ss.ASSEMBLY_DISTRICT,
    ss.B7SC,
    CASE 
        WHEN COALESCE(sn.STREET_NAME, fn.STREET_NAME) IS NULL 
        THEN TRUE 
        ELSE FALSE 
    END as MISSING_STREET_NAME
FROM special_sedat ss
LEFT JOIN street_names sn ON ss.B7SC = sn.B7SC
LEFT JOIN feature_names fn ON ss.B7SC = fn.B7SC
