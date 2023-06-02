DROP TABLE IF EXISTS _dot_publicparking;

SELECT
    uid,
    source,
    site as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    address,
    NULL as city,
    NULL as zipcode,
    boroname as boro,
    borocode,
    NULL as bin,
    bbl,
    'Public Parking' as factype,
    'Parking Lots and Garages' as facsubgrp,
    'NYC Department of Transportation' as opname,
    'NYCDOT' as opabbrev,
    'NYCDOT' as overabbrev,
    NULL as capacity,
    NULL as captype,
    wkt::geometry as wkb_geometry,
    geo_1b,
    geo_bl,
    NULL as geo_bn
INTO _dot_publicparking
FROM dot_publicparking;

CALL append_to_facdb_base('_dot_publicparking');
