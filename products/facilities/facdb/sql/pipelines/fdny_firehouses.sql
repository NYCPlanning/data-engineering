DROP TABLE IF EXISTS _fdny_firehouses;
SELECT
    uid,
    source,
    facilityname AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    facilityaddress AS address,
    NULL AS city,
    postcode AS zipcode,
    borough AS boro,
    NULL AS borocode,
    bin,
    bbl,
    'Firehouse' AS factype,
    'Fire Services' AS facsubgrp,
    'NYC Fire Department' AS opname,
    'FDNY' AS opabbrev,
    'FDNY' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    wkt::geometry AS wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn
INTO _fdny_firehouses
FROM fdny_firehouses;

CALL append_to_facdb_base('_fdny_firehouses');
