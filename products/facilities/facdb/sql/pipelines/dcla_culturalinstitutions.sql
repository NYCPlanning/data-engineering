DROP TABLE IF EXISTS _dcla_culturalinstitutions;
SELECT
    uid,
    source,
    initcap(organization_name) AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    cleaned_address AS address,
    city,
    zipcode,
    borough AS boro,
    NULL AS borocode,
    bin,
    bbl,
    coalesce(discipline, 'Unspecified Discipline') AS factype,
    (CASE
        WHEN discipline LIKE '%Museum%' THEN 'Museums'
        ELSE 'Other Cultural Institutions'
    END) AS facsubgrp,
    organization_name AS opname,
    'Non-public' AS opabbrev,
    'NYCDCLA' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    wkt::geometry AS wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn
INTO _dcla_culturalinstitutions
FROM dcla_culturalinstitutions;

CALL append_to_facdb_base('_dcla_culturalinstitutions');
