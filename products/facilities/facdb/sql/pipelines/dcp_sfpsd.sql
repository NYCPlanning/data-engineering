DROP TABLE IF EXISTS _dcp_sfpsd;
SELECT
    uid,
    source,
    facname,
    addressnum,
    streetname,
    address,
    city,
    split_part(zipcode, '.', 1) AS zipcode,
    boro,
    split_part(borocode, '.', 1) AS borocode,
    bin,
    bbl,
    factype,
    facsubgrp,
    opname,
    opabbrev,
    overabbrev,
    NULL AS capacity,
    NULL AS captype,
    the_geom::geometry AS wkb_geometry,
    NULL AS geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _dcp_sfpsd
FROM dcp_sfpsd;

CALL append_to_facdb_base('_dcp_sfpsd');
