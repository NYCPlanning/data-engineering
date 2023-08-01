DROP TABLE IF EXISTS _fdny_firehouses;
SELECT
    uid,
    source,
    facilityname as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    facilityaddress as address,
    NULL as city,
    postcode as zipcode,
    borough as boro,
    NULL as borocode,
    bin,
    bbl,
    'Firehouse' as factype,
    'Fire Services' as facsubgrp,
    'NYC Fire Department' as opname,
    'FDNY' as opabbrev,
    'FDNY' as overabbrev,
    NULL as capacity,
    NULL as captype,
    wkt::geometry as wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn
INTO _fdny_firehouses
FROM fdny_firehouses;

CALL append_to_facdb_base('_fdny_firehouses');
