DROP TABLE IF EXISTS _nysoasas_programs;

SELECT
    uid,
    source,
    program_name as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    provider_street as address,
    provider_city as city,
    provider_zip_code as zipcode,
    provider_county as boro,
    NULL as borocode,
    NULL as bin,
    NULL as bbl,
    service_type as factype,
    'Substance Use Disorder Treatment Programs' as facsubgrp,
    provider_name as opname,
    NULL as opabbrev,
    'NYSOASAS' as overabbrev,
    NULL as capacity,
    NULL as captype,
    NULL as wkb_geometry,
    geo_1b,
    NULL as geo_bl,
    NULL as geo_bn
INTO _nysoasas_programs
FROM nysoasas_programs;

CALL append_to_facdb_base('_nysoasas_programs');
