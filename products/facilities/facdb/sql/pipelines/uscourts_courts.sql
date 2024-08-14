DROP TABLE IF EXISTS _uscourts_courts;
SELECT
    uid,
    source,
    buildingname AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    buildingaddress AS address,
    buildingcity AS city,
    zipcode,
    buildingcity AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    courttype AS factype,
    (CASE
        WHEN upper(courttype) LIKE '%COURT%'
            THEN 'Courthouses and Judicial'
        ELSE 'Legal and Intervention Services'
    END) AS facsubgrp,
    officename AS opname,
    NULL AS opabbrev,
    'USCOURTS' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    wkt::geometry AS wkb_geometry,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _uscourts_courts
FROM uscourts_courts
WHERE buildingcity IN ('New York', 'Brooklyn');

CALL append_to_facdb_base('_uscourts_courts');
