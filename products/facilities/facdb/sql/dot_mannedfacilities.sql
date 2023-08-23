DROP TABLE IF EXISTS _dot_mannedfacilities;

SELECT
    uid,
    source,
    (CASE
        WHEN operations IS NOT NULL THEN operations
        ELSE division
    END) as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    (CASE
        WHEN address IS NOT NULL THEN address
        ELSE site
    END) as address,
    NULL as city,
    NULL as zipcode,
    boroname as boro,
    borocode,
    NULL as bin,
    bbl,
    (CASE
        WHEN operations LIKE '%Asphalt%' THEN 'Asphalt Plant'
        WHEN division LIKE '%RRM%'
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
    END) as factype,
    (CASE
        WHEN operations LIKE '%Asphalt%' THEN 'Material Supplies'
        ELSE 'Other Transportation'
    END) as facsubgrp,
    'NYC Department of Transportation' as opname,
    'NYCDOT' as opabbrev,
    'NYCDOT' as overabbrev,
    NULL as capacity,
    NULL as captype,
    wkt::geometry as wkb_geometry,
    geo_1b,
    geo_bl,
    NULL as geo_bn
INTO _dot_mannedfacilities
FROM dot_mannedfacilities;

CALL append_to_facdb_base('_dot_mannedfacilities');
