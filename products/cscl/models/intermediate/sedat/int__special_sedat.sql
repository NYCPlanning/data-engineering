WITH special_sedat AS (
    SELECT * FROM {{ source("recipe_sources", "dcp_cscl_specialsedat") }}
),
feature_name AS (
    SELECT * FROM {{ ref("stg__facecode_and_featurename") }}
)
SELECT
    special_sedat.lionkey,
    special_sedat.parity,
    feature_name.lookup_key AS street_name,
    CASE
        WHEN special_sedat.side = '1' THEN 'L'
        WHEN special_sedat.side = '2' THEN 'R'
    END AS side_of_street,
    TRIM(special_sedat.lowaddress) AS lowaddress,
    special_sedat.low_addr_suffix,
    TRIM(special_sedat.highaddress) AS highaddress,
    special_sedat.high_addr_suffix,
    special_sedat.election_district,
    special_sedat.assembly_district,
    special_sedat.b7sc
FROM special_sedat
LEFT JOIN feature_name ON special_sedat.b7sc = feature_name.b7sc
