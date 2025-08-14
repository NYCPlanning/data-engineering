{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid', 'boroughcode']},
    ]
) }}

WITH sedat AS (
    SELECT * FROM {{ source("recipe_sources", "dcp_cscl_sedat") }}
),

preferred_lgc AS (
    SELECT 
        SEGMENTID,
        CONCAT(B5SC, LGC) as B7SC,
        SUBSTR(B5SC, 1, 1) as BOROUGH_CODE
    FROM {{ source("recipe_sources", "dcp_cscl_segment_lgc") }}
    WHERE PREFERRED_LGC_FLAG = 'Y'
),

sedat_with_b7sc AS (
    SELECT 
        s.*,
        SUBSTR(s.LIONKEY, 1, 1) as BOROUGHCODE,
        pl.B7SC as PREFERRED_B7SC
    FROM sedat s
    LEFT JOIN preferred_lgc pl 
        ON s.SEGMENTID = pl.SEGMENTID
        AND SUBSTR(s.LIONKEY, 1, 1) = pl.BOROUGH_CODE
)

SELECT 
    sb.LIONKEY,
    sb.PARITY,
    sb.SEGMENTID,
    sb.BOROUGHCODE,
    sn.LOOKUP_KEY as STREET_NAME,
    CASE 
        WHEN sb.SIDE = '1' THEN 'L'
        WHEN sb.SIDE = '2' THEN 'R'
    END as SIDE_OF_STREET,
    sb.LOWADDRESS,
    sb.LOW_ADDR_SUFFIX,
    sb.HIGHADDRESS,
    sb.HIGH_ADDR_SUFFIX,
    sb.ELECTION_DISTRICT,
    sb.ASSEMBLY_DISTRICT,
    sb.PREFERRED_B7SC
FROM sedat_with_b7sc sb
LEFT JOIN {{ source("recipe_sources", "dcp_cscl_streetname") }} sn
    ON sb.PREFERRED_B7SC = sn.B7SC
    AND sn.PRINCIPAL_FLAG = 'Y'
