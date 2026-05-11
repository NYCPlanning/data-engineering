DROP TABLE IF EXISTS _dycd_service_sites;

SELECT
    uid,
    source,
    program_site_name AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    address,
    city,
    zipcode,
    borough AS boro,
    NULL AS borocode,
    bin,
    bbl,
    (CASE
        WHEN service_category ILIKE '%AFTERSCHOOL%' AND program_type ~* 'COMPASS Middle School'
            THEN 'COMPASS'
        WHEN service_category ILIKE '%AFTERSCHOOL%' AND program_type ~* 'COMPASS Elementary'
            THEN 'COMPASS ELEMENTARY'
        WHEN service_category ILIKE '%AFTERSCHOOL%' AND program_type ~* 'COMPASS Explore'
            THEN 'COMPASS EXPLORE'
        WHEN service_category ILIKE '%AFTERSCHOOL%' AND program_type ~* 'COMPASS High School'
            THEN 'COMPASS HIGH'
        WHEN service_category ILIKE '%AFTERSCHOOL%' AND program_type ~* 'COMPASS SONYC Pilot'
            THEN 'COMPASS'
        WHEN service_category ILIKE '%AFTERSCHOOL%' AND program_type ~* 'Beacon'
            THEN 'BEACON'
        WHEN service_category ILIKE '%AFTERSCHOOL%' AND program_type ~* 'Cornerstone'
            THEN 'CORNERSTONE'
        WHEN service_category ILIKE '%AFTERSCHOOL%' AND program_type ~* 'High School'
            THEN 'HIGH SCHOOL AFTERSCHOOL PROGRAMS'
        WHEN service_category ILIKE '%AFTERSCHOOL%' AND program_type ~* 'Learn and Earn'
            THEN 'LEARN AND EARN'
        WHEN service_category ILIKE '%AFTERSCHOOL%' AND program_type ~* 'Adolescent Literacy'
            THEN 'ADOLESCENT LITERACY'
        WHEN service_category IS NOT NULL THEN service_category
        WHEN program_type ~* 'Transitional Independent Living \(TIL\)'
            THEN 'Transitional Independent Living'
        WHEN program_type ~* 'Transitional Independent Living \(HYA\)'
            THEN 'Transitional Independent Living'
        WHEN program_type ~* 'Adult Literacy Pilot Project'
            THEN 'Adult Literacy Pilot Program'
        WHEN program_type ~* 'Services for Immigrants'
            THEN 'Immigrant Services'
        WHEN program_type ~* 'Immigrant Workers'
            THEN 'Immigrant Workers'
        WHEN program_type ~* 'Crisis Shelters'
            THEN 'Crisis Shelters'
        WHEN program_type ~* 'Victims of Domestic Violence and Trafficking'
            THEN 'VICTIM SERVICES, DOMESTIC VIOLENCE'
        WHEN program_type ~* 'Legal Services For Immigrant Youth'
            THEN 'Legal Services for Immigrant Youth'
        WHEN program_type ~* 'COMPASS Horizon'
            THEN 'COMPASS'
        ELSE NULL
    END) AS factype,
    (CASE
        WHEN
            service_category ILIKE '%AFTERSCHOOL%' AND (
                program_type ~* 'COMPASS'
                OR program_type ~* 'Beacon'
                OR program_type ~* 'Cornerstone'
                OR program_type ~* 'High School'
            )
            THEN 'After-School Programs'
        WHEN
            service_category ILIKE '%AFTERSCHOOL%' AND (
                program_type ~* 'Learn and Earn'
                OR program_type ~* 'Adolescent Literacy'
            )
            THEN 'Youth Centers, Literacy Programs, and Job Training Services'
        WHEN
            program_type ~* 'Beacon'
            OR program_type ~* 'High-School Aged Youth'
            OR program_type ~* 'Middle School Youth'
            OR program_type ~* 'Teen Action Program'
            OR program_type ~* 'After-School Programs'
            THEN 'After-School Programs'
        WHEN program_type ~* 'COMPASS Horizon'
            THEN 'After-School Programs'
        WHEN
            program_type ~* 'Services for Immigrants'
            OR program_type ~* 'Immigrant Workers'
            OR program_type ~* 'Legal Services For Immigrant Youth'
            THEN 'Immigrant Services'
        WHEN program_type ~* 'Victims of Domestic Violence and Trafficking'
            THEN 'LEGAL AND INTERVENTION SERVICES'
        WHEN service_category IS NOT NULL
            THEN 'Youth Centers, Literacy Programs, and Job Training Services'
        WHEN
            program_type ~* 'Transitional Independent Living'
            OR program_type ~* 'Adult Literacy Pilot Project'
            OR program_type ~* 'Crisis Shelters'
            THEN 'Youth Centers, Literacy Programs, and Job Training Services'
        ELSE NULL
    END) AS facsubgrp,
    provider AS opname,
    NULL AS opabbrev,
    'NYCDYCD' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    ST_POINT(longitude::double precision, latitude::double precision) AS wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn
INTO _dycd_service_sites
FROM dycd_service_sites
WHERE fiscalyear = (SELECT MAX(d.fiscalyear) FROM dycd_service_sites AS d);

CALL APPEND_TO_FACDB_BASE('_dycd_service_sites');
