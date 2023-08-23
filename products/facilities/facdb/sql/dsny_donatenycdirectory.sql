DROP TABLE IF EXISTS _dsny_donatenycdirectory;
SELECT uid,
    source,
    CONCAT(site, ' Textile Drop-off Site') as facname,
    NULL as addressnum,
    NULL as streetname,
    address,
    NULL as city,
    NULL as zipcode,
    borough as boro,
    NULL as borocode,
    bin,
    bbl,
    'Textiles' as factype,
    'DSNY Drop-off Facility' as facsubgrp,
    site as opname,
    NULL as opabbrev,
    'NYCDSNY' as overabbrev,
    NULL as capacity,
    NULL as captype,
    wkt::geometry as wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn
INTO _dsny_donatenycdirectory
FROM dsny_donatenycdirectory;

CALL append_to_facdb_base('_dsny_donatenycdirectory');
