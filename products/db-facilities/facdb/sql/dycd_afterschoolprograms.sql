DROP TABLE IF EXISTS _dycd_afterschoolprograms;

SELECT
    uid,
    source,
    site_name AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    address,
    NULL AS city,
    NULL AS borocode,
    bin,
    bbl,
    agency AS opname,
    NULL AS opabbrev,
    'NYCDYCD' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    geo_1b,
    geo_bl,
    geo_bn,
    coalesce(postcode, zipcode) AS zipcode,
    split_part(borough__community, ',', 1) AS boro,
    replace(program, 'NDA Immigrats', 'NDA Immigrants') || ': ' || program_type AS factype,
    (CASE
        WHEN
            program ~* 'Beacon'
            OR program ~* 'High-School Aged Youth'
            OR program ~* 'Middle School Youth'
            OR program ~* 'Teen Action Program'
            OR program ~* 'After-School Programs'
            THEN 'After-School Programs'
        WHEN program_type ~* 'Immigrant Support Services'
            THEN 'Immigrant Services'
        ELSE 'Youth Centers, Literacy Programs, and Job Training Services'
    END) AS facsubgrp,
    st_point(longitude::double precision, latitude::double precision) AS wkb_geometry
INTO _dycd_afterschoolprograms
FROM dycd_afterschoolprograms;

CALL append_to_facdb_base('_dycd_afterschoolprograms');
