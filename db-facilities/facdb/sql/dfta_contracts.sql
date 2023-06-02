DROP TABLE IF EXISTS _dfta_contracts;
SELECT uid,
    source,
    initcap(sponsor_name) as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    program_address as address,
    NULL as city,
    program_zipcode as zipcode,
    NULL as boro,
    NULL as borocode,
    NULL as bin,
    NULL as bbl,
    (
        CASE
            WHEN (
                contract_type LIKE '%INNOVATIVE%'
                AND RIGHT(provider_id, 2) <> '01'
            )
            OR (
                contract_type LIKE '%NEIGHBORHOOD%'
                AND RIGHT(provider_id, 2) <> '01'
            )
            OR (contract_type LIKE '%INNOVATIVE%')
            OR (contract_type LIKE '%NEIGHBORHOOD%') THEN 'Senior Center'
            WHEN contract_type LIKE '%MEALS%' THEN initcap(contract_type)
            ELSE 'Senior Services'
        END
    ) as factype,
    'Senior Services' as facsubgrp,
    initcap(sponsor_name) as opname,
    'Non-public' as opabbrev,
    'NYCDFTA' as overabbrev,
    NULL as capacity,
    NULL as captype,
    NULL as wkb_geometry,
    geo_1b,
    NULL as geo_bl,
    NULL as geo_bn INTO _dfta_contracts
FROM dfta_contracts;
CALL append_to_facdb_base('_dfta_contracts');
