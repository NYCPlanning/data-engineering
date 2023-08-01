DROP TABLE IF EXISTS _nycourts_courts;

WITH
colocated_summons AS(
    SELECT
    a.uid as summons_uid,
    b.uid as non_summons_uid,
    a.name as summons_name,
    b.name as non_summons_name
    FROM nycourts_courts a
    JOIN nycourts_courts b
    ON a.address = b.address
    WHERE a.name ~* 'Summons'
    AND b.name !~* 'Summons'
    AND a.uid <> b.uid
)
SELECT
    uid,
    source,
    (CASE
        WHEN uid IN (SELECT non_summons_uid FROM colocated_summons)
        THEN name||' (Colocated Summons Court)'
        ELSE REPLACE(
                REPLACE(
                    REPLACE(name,'( ','(')
                , ' )',')')
            ,'The ','')
    END) as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    address as address,
    NULL as city,
    zipcode,
    borough as boro,
    NULL as borocode,
    NULL as bin,
    NULL as bbl,
    'Courthouse' as factype,
    'Courthouses and Judicial' as facsubgrp,
    'NYS Unified Court System' as opname,
    'NYCOURTS' as opabbrev,
    'NYCOURTS' as overabbrev,
    NULL as capacity,
    NULL as captype,
    NULL as wkb_geometry,
    geo_1b,
    NULL as geo_bl,
    NULL as geo_bn
INTO _nycourts_courts
FROM nycourts_courts
WHERE uid NOT IN (SELECT summons_uid FROM colocated_summons)
;

CALL append_to_facdb_base('_nycourts_courts');
