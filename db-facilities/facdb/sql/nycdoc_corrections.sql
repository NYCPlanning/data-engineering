DROP TABLE IF EXISTS _nycdoc_corrections;

SELECT
    uid,
    source,
    name as facname,
    house_number as addressnum,
    street_name as streetname,
    address1 as address,
    NULL as city,
    zipcode,
    NULL as boro,
    NULL as borocode,
    NULL as bin,
    NULL as bbl,
    'Correctional Facility' as factype,
    'Detention and Correctional' as facsubgrp,
    'NYC Department of Correction' as opname,
    'NYCDOC' as opabbrev,
    'NYCDOC' as overabbrev,
    NULL as capacity,
    NULL as captype,
    NULL as wkb_geometry,
    geo_1b,
    NULL as geo_bl,
    NULL as geo_bn
INTO _nycdoc_corrections
FROM nycdoc_corrections;

CALL append_to_facdb_base('_nycdoc_corrections');
