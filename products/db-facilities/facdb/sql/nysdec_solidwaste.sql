DROP TABLE IF EXISTS _nysdec_solidwaste;

SELECT
    uid,
    source,
    facility_name as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    location_address as address,
    city,
    zip_code as zipcode,
    NULL as boro,
    NULL as borocode,
    NULL as bin,
    NULL as bbl,
    (CASE
        WHEN activity_desc LIKE '%C&D%' THEN 'Construction and Demolition Processing'
        WHEN activity_desc LIKE '%Composting%' THEN 'Composting'
        WHEN activity_desc LIKE '%Other%' THEN 'Other Solid Waste Processing'
        WHEN activity_desc LIKE '%RHRF%' THEN 'Recyclables Handling and Recovery'
        WHEN activity_desc LIKE '%medical%' THEN 'Regulated Medical Waste'
        WHEN activity_desc LIKE '%Transfer%' THEN 'Transfer Station'
        ELSE initcap(trim(split_part(split_part(activity_desc,';',1),'-',1),' '))
	END) as factype,
    (CASE
        WHEN activity_desc LIKE '%Transfer%' THEN 'Solid Waste Transfer and Carting'
        ELSE 'Solid Waste Processing'
	END) as facsubgrp,
    (CASE
        WHEN owner_type = 'Municipal' THEN 'NYC Department of Sanitation'
        WHEN owner_name IS NOT NULL THEN owner_name
        ELSE 'Unknown'
	END) as opname,
    (CASE
        WHEN owner_type = 'Municipal' THEN 'NYCDSNY'
        ELSE 'Non-public'
	END) as opabbrev,
    (CASE
        WHEN owner_type = 'Municipal' THEN 'NYCDSNY'
        ELSE 'NYSDEC'
	END) as overabbrev,
    NULL as capacity,
    NULL as captype,
    NULL as wkb_geometry,
    geo_1b,
    NULL as geo_bl,
    NULL as geo_bn
INTO _nysdec_solidwaste
FROM nysdec_solidwaste;

CALL append_to_facdb_base('_nysdec_solidwaste');
