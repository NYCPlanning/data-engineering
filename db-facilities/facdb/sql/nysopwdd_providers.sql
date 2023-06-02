DROP TABLE IF EXISTS _nysopwdd_providers;
SELECT
    uid,
    source,
    initcap(service_provider_agency) as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    (
        CASE
        WHEN street_address_line_2 IS NOT NULL
        THEN street_address || ' ' || street_address_line_2
        ELSE street_address
        END
    ) as address,
    city as city,
    zip_code as zipcode,
    county as boro,
    NULL as borocode,
    NULL as bin,
    NULL as bbl,
    'Programs for People with Disabilities' as factype,
    'Programs for People with Disabilities' as facsubgrp,
    initcap(service_provider_agency) as opname,
    'NYS Office for People With Developmental Disabilities' as overagency,
    'Non-public' as opabbrev,
    'NYSOPWDD' as overabbrev,
    NULL as capacity,
    NULL as captype,
    NULL as wkb_geometry,
    geo_1b,
    NULL as geo_bl,
    NULL as geo_bn
INTO _nysopwdd_providers
FROM nysopwdd_providers;

CALL append_to_facdb_base('_nysopwdd_providers');
