DROP TABLE IF EXISTS _dycd_afterschoolprograms;

SELECT
    uid,
    source,
    site_name as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    address,
    NULL as city,
    coalesce(postcode, zipcode) as zipcode,
    SPLIT_PART(borough__community, ',', 1) as boro,
    NULL as borocode,
    bin,
    bbl,
    REPLACE(program, 'NDA Immigrats', 'NDA Immigrants')||': '||program_type as factype,
    (CASE
		WHEN program ~* 'Beacon'
			OR program ~* 'High-School Aged Youth'
			OR program ~* 'Middle School Youth'
			OR program ~* 'Teen Action Program'
            OR program ~* 'After-School Programs'
		    THEN 'After-School Programs'
		WHEN program_type ~* 'Immigrant Support Services'
			THEN 'Immigrant Services'
			ELSE 'Youth Centers, Literacy Programs, and Job Training Services'
	END) as facsubgrp,
    agency as opname,
    NULL as opabbrev,
    'NYCDYCD' as overabbrev,
    NULL as capacity,
    NULL as captype,
    ST_POINT(longitude::double precision, latitude::double precision) as wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn
INTO _dycd_afterschoolprograms
FROM dycd_afterschoolprograms;

CALL append_to_facdb_base('_dycd_afterschoolprograms');
