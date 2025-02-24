DROP TABLE IF EXISTS _dot_mannedfacilities;

SELECT
    uid,
    source,
    coalesce(operations, division) AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    coalesce(address, site) AS address,
    NULL AS city,
    NULL AS zipcode,
    boroname AS boro,
    borocode,
    NULL AS bin,
    bbl,
    (CASE
        WHEN operations LIKE '%Asphalt%' THEN 'Asphalt Plant'
        WHEN
            division LIKE '%RRM%'
            OR division LIKE '%SIM%'
            OR division LIKE '%OCMC%'
            OR division LIKE '%HIQA%'
            OR division LIKE '%TMC%'
            OR division LIKE '%JETS%'
            OR division LIKE '%JETS%'
            OR division LIKE '%Multiple%'
            OR division LIKE '%External Affairs%'
            OR division LIKE '%Services%'
            THEN 'Maintenance, Management, and Operations'
        ELSE 'Manned Transportation Facility'
    END) AS factype,
    (CASE
        WHEN operations LIKE '%Asphalt%' THEN 'Material Supplies'
        ELSE 'Other Transportation'
    END) AS facsubgrp,
    'NYC Department of Transportation' AS opname,
    'NYCDOT' AS opabbrev,
    'NYCDOT' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    geometry AS wkb_geometry,
    geo_1b,
    geo_bl,
    NULL AS geo_bn
INTO _dot_mannedfacilities
FROM dot_mannedfacilities;

CALL append_to_facdb_base('_dot_mannedfacilities');
