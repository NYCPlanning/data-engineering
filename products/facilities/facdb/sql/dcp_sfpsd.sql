DROP TABLE IF EXISTS _dcp_sfpsd;
SELECT
    uid,
    source,
    facname,
    addressnum,
    streetname,
    address,
    city,
    SPLIT_PART(zipcode, '.', 1) AS zipcode,
    boro,
    SPLIT_PART(borocode, '.', 1) AS borocode,
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

CALL APPEND_TO_FACDB_BASE('_dcp_sfpsd');
