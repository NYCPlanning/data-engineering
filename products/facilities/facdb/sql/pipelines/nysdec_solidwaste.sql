DROP TABLE IF EXISTS _nysdec_solidwaste;

SELECT
    uid,
    source,
    facility_name AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    location_address AS address,
    city,
    zip_code AS zipcode,
    NULL AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    (CASE
        WHEN activity_desc LIKE '%C&D%' THEN 'Construction and Demolition Processing'
        WHEN activity_desc LIKE '%Composting%' THEN 'Composting'
        WHEN activity_desc LIKE '%Other%' THEN 'Other Solid Waste Processing'
        WHEN activity_desc LIKE '%RHRF%' THEN 'Recyclables Handling and Recovery'
        WHEN activity_desc LIKE '%medical%' THEN 'Regulated Medical Waste'
        WHEN activity_desc LIKE '%Transfer%' THEN 'Transfer Station'
        ELSE initcap(trim(split_part(split_part(activity_desc, ';', 1), '-', 1), ' '))
    END) AS factype,
    (CASE
        WHEN activity_desc LIKE '%Transfer%' THEN 'Solid Waste Transfer and Carting'
        ELSE 'Solid Waste Processing'
    END) AS facsubgrp,
    (CASE
        WHEN owner_type = 'Municipal' THEN 'NYC Department of Sanitation'
        WHEN owner_name IS NOT NULL THEN owner_name
        ELSE 'Unknown'
    END) AS opname,
    (CASE
        WHEN owner_type = 'Municipal' THEN 'NYCDSNY'
        ELSE 'Non-public'
    END) AS opabbrev,
    (CASE
        WHEN owner_type = 'Municipal' THEN 'NYCDSNY'
        ELSE 'NYSDEC'
    END) AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    geom AS wkb_geometry,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _nysdec_solidwaste
FROM nysdec_solidwaste;

CALL append_to_facdb_base('_nysdec_solidwaste');
