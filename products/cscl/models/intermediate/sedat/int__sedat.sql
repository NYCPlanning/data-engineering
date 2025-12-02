WITH sedat AS (
    SELECT
        LEFT(lionkey, 1) AS borough_code,
        *
    FROM {{ source("recipe_sources", "dcp_cscl_sedat") }}
),
preferred_lgc AS (
    SELECT
        segmentid,
        CONCAT(b5sc, lgc) AS b7sc,
        LEFT(b5sc, 1) AS borough_code
    FROM {{ source("recipe_sources", "dcp_cscl_segment_lgc") }}
    WHERE preferred_lgc_flag = 'Y'
),
streetname AS (
    SELECT * FROM {{ source("recipe_sources", "dcp_cscl_streetname") }}
    WHERE principal_flag = 'Y'
)
SELECT
    sedat.lionkey,
    sedat.parity,
    streetname.lookup_key AS street_name,
    CASE
        WHEN sedat.side = '1' THEN 'L'
        WHEN sedat.side = '2' THEN 'R'
    END AS side_of_street,
    TRIM(sedat.lowaddress) AS lowaddress,
    sedat.low_addr_suffix,
    TRIM(sedat.highaddress) AS highaddress,
    sedat.high_addr_suffix,
    sedat.election_district,
    sedat.assembly_district,
    preferred_lgc.b7sc
FROM sedat
LEFT JOIN preferred_lgc
    ON
        sedat.segmentid = preferred_lgc.segmentid
        AND sedat.borough_code = preferred_lgc.borough_code
LEFT JOIN streetname
    ON preferred_lgc.b7sc = streetname.b7sc
