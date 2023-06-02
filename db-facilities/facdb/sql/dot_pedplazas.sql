DROP TABLE IF EXISTS _dot_pedplazas;

SELECT
    uid,
    source,
    plazaname as facname,
    NULL as addressnum,
    NULL as streetname,
    fromstreet || ' and ' || tostreet as address,
    NULL as city,
    NULL as zipcode,
    boroname as boro,
    borocode,
    NULL as bin,
    NULL as bbl,
    'Pedestrian Plaza' as factype,
    'Streetscapes, Plazas, and Malls' as facsubgrp,
    'NYC Department of Transportation' as opname,
    'NYCDOT' as opabbrev,
    'NYCDOT' as overabbrev,
    NULL as capacity,
    NULL as captype,
    wkt::geometry as wkb_geometry,
    NULL as geo_1b,
    NULL as geo_bl,
    NULL as geo_bn
INTO _dot_pedplazas
FROM dot_pedplazas;

CALL append_to_facdb_base('_dot_pedplazas');
