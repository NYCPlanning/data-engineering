DROP TABLE IF EXISTS _sbs_workforce1;
SELECT
    uid,
    source,
    name as facname,
    number as addressnum,
    street as streetname,
    number||' '||street as address,
    city,
    postcode as zipcode,
    borough as boro,
    NULL as borocode,
    bin,
    bbl,
    location_type as factype,
    'Workforce Development' as facsubgrp,
    'NYC Department of Small Business Services' as opname,
    'NYCSBS' as opabbrev,
    'NYCSBS' as overabbrev,
    NULL as capacity,
    NULL as captype,
    wkt::geometry as wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn
INTO _sbs_workforce1
FROM sbs_workforce1;

CALL append_to_facdb_base('_sbs_workforce1');
