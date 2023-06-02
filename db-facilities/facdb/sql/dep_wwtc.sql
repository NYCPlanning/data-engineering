DROP TABLE IF EXISTS _dep_wwtc;
SELECT
    uid,
    source,
    name as facname,
    house_number as addressnum,
    street_name as streetname,
    address,
    NULL as city,
    zipcode,
    NULL as boro,
    NULL as borocode,
    NULL as bin,
    NULL as bbl,
    'Wastewater Treatment Plant' as factype,
    'Wastewater and Pollution Control' as facsubgrp,
    'NYC Department of Environmental Protection' as opname,
    'NYCDEP' as opabbrev,
    'NYCDEP' as overabbrev,
    NULL as capacity,
    NULL as captype,
    NULL as wkb_geometry,
    geo_1b,
    NULL as geo_bl,
    NULL as geo_bn
INTO _dep_wwtc
FROM dep_wwtc;

CALL append_to_facdb_base('_dep_wwtc');
