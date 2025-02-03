DROP TABLE IF EXISTS _nysparks_historicplaces;
SELECT
    uid,
    source,
    historicname AS facname,
    NULL AS addressnum,
    NULL AS streetname,
    NULL AS address,
    NULL AS city,
    NULL AS zipcode,
    countyname AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    'State Historic Place' AS factype,
    'Historical Sites' AS facsubgrp,
    NULL AS opname,
    'The New York State Office of Parks, Recreation and Historic Preservation' AS overagency,
    NULL AS opabbrev,
    'NYSOPRHP' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    geometry AS wkb_geometry,
    NULL AS geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _nysparks_historicplaces
FROM nysparks_historicplaces;

CALL append_to_facdb_base('_nysparks_historicplaces');
