DROP TABLE IF EXISTS _dcp_sfpsd;
SELECT
    uid,
    source,
    facname,
    addressnum,
    streetname,
    address,
    city,
    SPLIT_PART(zipcode,'.',1) as zipcode,
    boro,
    SPLIT_PART(borocode,'.',1) as borocode,
    bin,
    bbl,
    factype,
    facsubgrp,
    opname,
    opabbrev,
    overabbrev,
    NULL as capacity,
    NULL as captype,
    the_geom::geometry as wkb_geometry,
    NULL as geo_1b,
    NULL as geo_bl,
    NULL as geo_bn
INTO _dcp_sfpsd
FROM dcp_sfpsd;

CALL append_to_facdb_base('_dcp_sfpsd');
