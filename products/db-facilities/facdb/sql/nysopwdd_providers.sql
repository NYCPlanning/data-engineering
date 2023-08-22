DROP TABLE IF EXISTS _nysopwdd_providers;
SELECT
    uid,
    source,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    city AS city,
    zip_code AS zipcode,
    county AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    'Programs for People with Disabilities' AS factype,
    'Programs for People with Disabilities' AS facsubgrp,
    'NYS Office for People With Developmental Disabilities' AS overagency,
    'Non-public' AS opabbrev,
    'NYSOPWDD' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    NULL AS wkb_geometry,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn,
    initcap(service_provider_agency) AS facname,
    (
        CASE
            WHEN street_address_line_2 IS NOT NULL
                THEN street_address || ' ' || street_address_line_2
            ELSE street_address
        END
    ) AS address,
    initcap(service_provider_agency) AS opname
INTO _nysopwdd_providers
FROM nysopwdd_providers;

CALL append_to_facdb_base('_nysopwdd_providers');
