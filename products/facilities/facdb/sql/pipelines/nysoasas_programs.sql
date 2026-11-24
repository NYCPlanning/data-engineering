DROP TABLE IF EXISTS _nysoasas_programs;

SELECT
    uid,
    source,
    program_name AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    program_street_address AS address,
    program_city AS city,
    program_zip_code AS zipcode,
    program_county AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    service_type AS factype,
    'Substance Use Disorder Treatment Programs' AS facsubgrp,
    provider_name AS opname,
    NULL AS opabbrev,
    'NYSOASAS' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    NULL AS wkb_geometry,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _nysoasas_programs
FROM nysoasas_programs;

CALL append_to_facdb_base('_nysoasas_programs');
