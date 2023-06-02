DROP TABLE IF EXISTS _nycha_policeservice;

SELECT
    uid,
    source,
    initcap(psa) as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    address as address,
    NULL as city,
    zipcode,
    borough as boro,
    NULL as borocode,
    NULL as bin,
    NULL as bbl,
    'NYCHA Police Service' as factype,
    'Police Services' as facsubgrp,
    'NYC Housing Authority' as opname,
    'NYCHA' as opabbrev,
    'NYCHA' as overabbrev,
    NULL as capacity,
    NULL as captype,
    wkt::geometry as wkb_geometry,
    geo_1b,
    NULL as geo_bl,
    NULL as geo_bn
INTO _nycha_policeservice
FROM nycha_policeservice;

CALL append_to_facdb_base('_nycha_policeservice');
