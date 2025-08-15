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
        segmentid,
        CONCAT(b5sc, lgc) AS b7sc,
        SUBSTR(b5sc, 1, 1) AS borough_code
    FROM {{ source("recipe_sources", "dcp_cscl_segment_lgc") }}
    WHERE preferred_lgc_flag = 'Y'
),

sedat_with_b7sc AS (
    SELECT
        s.*,
        SUBSTR(s.lionkey, 1, 1) AS boroughcode,
        pl.b7sc AS preferred_b7sc
    FROM sedat AS s
    LEFT JOIN preferred_lgc AS pl
        ON
            s.segmentid = pl.segmentid
            AND SUBSTR(s.lionkey, 1, 1) = pl.borough_code
)

SELECT
    sb.lionkey,
    sb.parity,
    sb.segmentid,
    sb.boroughcode,
    sn.lookup_key AS street_name,
    CASE
        WHEN sb.side = '1' THEN 'L'
        WHEN sb.side = '2' THEN 'R'
    END AS side_of_street,
    sb.lowaddress,
    sb.low_addr_suffix,
    sb.highaddress,
    sb.high_addr_suffix,
    sb.election_district,
    sb.assembly_district,
    sb.preferred_b7sc
FROM sedat_with_b7sc AS sb
LEFT JOIN {{ source("recipe_sources", "dcp_cscl_streetname") }} AS sn
    ON
        sb.preferred_b7sc = sn.b7sc
        AND sn.principal_flag = 'Y'
