SELECT bct2020 AS "BOROCT"
FROM {{ ref("cdbg_tracts") }}
WHERE eligibility_flag
