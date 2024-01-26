DROP TABLE IF EXISTS _dycd_service_sites;

SELECT
    uid,
    source,
    program_site_name as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    address,
    city,
    zipcode,
    borough as boro,
    NULL as borocode,
    bin,
    bbl,
    service_category as factype,
    (CASE
		WHEN program_type ~* 'Beacon'
			OR program_type ~* 'High-School Aged Youth'
			OR program_type ~* 'Middle School Youth'
			OR program_type ~* 'Teen Action Program'
            OR program_type ~* 'After-School Programs'
		    THEN 'After-School Programs'
		WHEN service_category ~* 'Immigrant Support Services'
			THEN 'Immigrant Services'
			ELSE 'Youth Centers, Literacy Programs, and Job Training Services'
	END) as facsubgrp,
    provider as opname,
    NULL as opabbrev,
    'NYCDYCD' as overabbrev,
    NULL as capacity,
    NULL as captype,
    ST_POINT(longitude::double precision, latitude::double precision) as wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn
INTO _dycd_service_sites
FROM dycd_service_sites
WHERE fiscalyear = (SELECT MAX(fiscalyear) FROM dycd_service_sites);;

CALL append_to_facdb_base('_dycd_service_sites');
