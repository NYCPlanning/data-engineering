DROP TABLE IF EXISTS _dot_mannedfacilities;

SELECT
    uid,
    source,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    NULL AS city,
    NULL AS zipcode,
    boroname AS boro,
    borocode,
    NULL AS bin,
    bbl,
    'NYC Department of Transportation' AS opname,
    'NYCDOT' AS opabbrev,
    'NYCDOT' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    wkt::geometry AS wkb_geometry,
    geo_1b,
    geo_bl,
    NULL AS geo_bn,
    (COALESCE(operations, division)) AS facname,
    (COALESCE(address, site)) AS address,
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
    END) AS facsubgrp
INTO _dot_mannedfacilities
FROM dot_mannedfacilities;

CALL APPEND_TO_FACDB_BASE('_dot_mannedfacilities');
