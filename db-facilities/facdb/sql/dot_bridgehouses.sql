DROP TABLE IF EXISTS _dot_bridgehouses;

SELECT
    uid,
    source,
    site as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    raw_address as address,
    NULL as city,
    NULL as zipcode,
    boroname as boro,
    borocode,
    NULL as bin,
    NULL as bbl,
    'Bridge House' as factype,
    'Other Transportation' as facsubgrp,
    'NYC Department of Transportation' as opname,
    'NYCDOT' as opabbrev,
    'NYCDOT' as overabbrev,
    NULL as capacity,
    NULL as captype,
    wkt::geometry as wkb_geometry,
    geo_1b,
    NULL as geo_bl,
    NULL as geo_bn
INTO _dot_bridgehouses
FROM dot_bridgehouses;

CALL append_to_facdb_base('_dot_bridgehouses');
