DROP TABLE IF EXISTS _nysdoccs_corrections;

SELECT
    uid,
    source,
    facility_name as facname,
    house_number as addressnum,
    street_name as streetname,
    address,
    NULL as city,
    zipcode,
    county as boro,
    NULL as borocode,
    NULL as bin,
    NULL as bbl,
    'Correctional Facility' as factype,
    'Detention and Correctional' as facsubgrp,
    'NYS Department of Corrections and Community Supervision' as opname,
    'NYSDOCCS' as opabbrev,
    'NYSDOCCS' as overabbrev,
    NULL as capacity,
    NULL as captype,
    NULL as wkb_geometry,
    geo_1b,
    NULL as geo_bl,
    NULL as geo_bn
INTO _nysdoccs_corrections
FROM nysdoccs_corrections;

CALL append_to_facdb_base('_nysdoccs_corrections');
