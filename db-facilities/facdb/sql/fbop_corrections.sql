DROP TABLE IF EXISTS _fbop_corrections;
SELECT
    uid,
    source,
    nametitle as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    address,
    city,
    zipcode,
    city as boro,
    NULL as borocode,
    NULL as bin,
    NULL as bbl,
    'Detention Center' as factype,
    'Detention and Correctional' as facsubgrp,
    'Federal Bureau of Prisons' as opname,
    'FBOP' as opabbrev,
    'FBOP' as overabbrev,
    NULL as capacity,
    NULL as captype,
    NULL as wkb_geometry,
    geo_1b,
    NULL as geo_bl,
    NULL as geo_bn
INTO _fbop_corrections
FROM fbop_corrections;

CALL append_to_facdb_base('_fbop_corrections');
