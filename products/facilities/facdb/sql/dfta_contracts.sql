DROP TABLE IF EXISTS _dfta_contracts;
SELECT
    uid,
    source,
    initcap(sponsorname) AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    programaddress AS address,
    NULL AS city,
    postcode AS zipcode,
    NULL AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    (
        CASE
            WHEN providertype LIKE '%OLDER ADULT CENTER%' THEN 'Senior Center'
            WHEN (
                providertype LIKE '%HOME DELIVERED MEAL%'
            )
            OR (
                providertype LIKE '%CITY MEALS ADMINISTRATIVE SERVICES%'
                AND sponsorname LIKE '%CITYMEALS-ON-WHEELS%'
            ) THEN 'Home Delivered Meals'
            ELSE 'Senior Services'
        END
    ) AS factype,
    'Senior Services' AS facsubgrp,
    initcap(sponsorname) AS opname,
    'Non-public' AS opabbrev,
    'NYCDFTA' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    NULL AS wkb_geometry,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _dfta_contracts
FROM dfta_contracts;
CALL append_to_facdb_base('_dfta_contracts');
