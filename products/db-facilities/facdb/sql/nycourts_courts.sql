DROP TABLE IF EXISTS _nycourts_courts;

WITH
colocated_summons AS (
    SELECT
        a.uid AS summons_uid,
        b.uid AS non_summons_uid,
        a.name AS summons_name,
        b.name AS non_summons_name
    FROM nycourts_courts AS a
    INNER JOIN nycourts_courts AS b
        ON a.address = b.address
    WHERE
        a.name ~* 'Summons'
        AND b.name !~* 'Summons'
        AND a.uid != b.uid
)

SELECT
    uid,
    source,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    address AS address,
    NULL AS city,
    zipcode,
    borough AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    'Courthouse' AS factype,
    'Courthouses and Judicial' AS facsubgrp,
    'NYS Unified Court System' AS opname,
    'NYCOURTS' AS opabbrev,
    'NYCOURTS' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    NULL AS wkb_geometry,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn,
    (CASE
        WHEN uid IN (SELECT non_summons_uid FROM colocated_summons)
            THEN name || ' (Colocated Summons Court)'
        ELSE REPLACE(
            REPLACE(
                REPLACE(name, '( ', '('),
                ' )', ')'
            ),
            'The ', ''
        )
    END) AS facname
INTO _nycourts_courts
FROM nycourts_courts
WHERE uid NOT IN (SELECT summons_uid FROM colocated_summons);

CALL APPEND_TO_FACDB_BASE('_nycourts_courts');
