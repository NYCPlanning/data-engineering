DROP TABLE IF EXISTS _nysparks_historicplaces;
SELECT
    uid,
    source,
    resource_name as facname,
    NULL as addressnum,
    NULL as streetname,
    NULL as address,
    NULL as city,
    NULL as zipcode,
    county as boro,
    NULL as borocode,
    NULL as bin,
    NULL as bbl,
    'State Historic Place' as factype,
    'Historical Sites' as facsubgrp,
    NULL as opname,
    'The New York State Office of Parks, Recreation and Historic Preservation' as overagency,
    NULL as opabbrev,
    'NYSOPRHP' as overabbrev,
    NULL as capacity,
    NULL as captype,
    wkt::geometry as wkb_geometry,
    NULL geo_1b,
    NULL as geo_bl,
    NULL as geo_bn
INTO _nysparks_historicplaces
FROM nysparks_historicplaces;

CALL append_to_facdb_base('_nysparks_historicplaces');
