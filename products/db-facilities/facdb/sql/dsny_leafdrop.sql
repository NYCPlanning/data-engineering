DROP TABLE IF EXISTS _dsny_leafdrop;
SELECT uid,
    source,
    CONCAT(site_name, ' Leaf Drop-off Site') as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    address,
    NULL as city,
    zipcode,
    borough as boro,
    NULL as borocode,
    bin,
    bbl,
    'Leaf' as factype,
    'DSNY Drop-off Facility' as facsubgrp,
    site_managed_by as opname,
    NULL as opabbrev,
    'NYCDSNY' as overabbrev,
    NULL as capacity,
    NULL as captype,
    wkt::geometry as wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn
INTO _dsny_leafdrop
FROM dsny_leafdrop;

CALL append_to_facdb_base('_dsny_leafdrop');
