DROP TABLE IF EXISTS _nysopwdd_providers;
SELECT
    uid,
    source,
    initcap(service_provider_agency) AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    (
        CASE
            WHEN street_address_line_2 IS NOT NULL
                THEN street_address || ' ' || street_address_line_2
            ELSE street_address
        END
    ) AS address,
    city,
    zip_code AS zipcode,
    county AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    'Programs for People with Disabilities' AS factype,
    'Programs for People with Disabilities' AS facsubgrp,
    initcap(service_provider_agency) AS opname,
    'NYS Office for People With Developmental Disabilities' AS overagency,
    'Non-public' AS opabbrev,
    'NYSOPWDD' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    NULL AS wkb_geometry,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _nysopwdd_providers
FROM nysopwdd_providers;

CALL append_to_facdb_base('_nysopwdd_providers');
