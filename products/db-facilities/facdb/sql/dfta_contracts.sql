DROP TABLE IF EXISTS _dfta_contracts;
SELECT
    uid,
    source,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    program_address AS address,
    NULL AS city,
    program_zipcode AS zipcode,
    NULL AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    'Senior Services' AS facsubgrp,
    'Non-public' AS opabbrev,
    'NYCDFTA' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    NULL AS wkb_geometry,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn,
    initcap(sponsor_name) AS facname,
    (
        CASE
            WHEN (
                contract_type LIKE '%INNOVATIVE%'
                AND right(provider_id, 2) != '01'
            )
            OR (
                contract_type LIKE '%NEIGHBORHOOD%'
                AND right(provider_id, 2) != '01'
            )
            OR (contract_type LIKE '%INNOVATIVE%')
            OR (contract_type LIKE '%NEIGHBORHOOD%') THEN 'Senior Center'
            WHEN contract_type LIKE '%MEALS%' THEN initcap(contract_type)
            ELSE 'Senior Services'
        END
    ) AS factype,
    initcap(sponsor_name) AS opname
INTO _dfta_contracts
FROM dfta_contracts;
CALL append_to_facdb_base('_dfta_contracts');
