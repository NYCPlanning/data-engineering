DROP TABLE IF EXISTS _dohmh_daycare;
WITH
inspection_dates AS (
    SELECT
        uid,
        (CASE
            WHEN UPPER(inspection_date) = 'NONE' THEN '01/01/2000'
			ELSE inspection_date
		END) as inspection_date
    FROM dohmh_daycare
),
latest_inspections AS(
    SELECT
        day_care_id,
        MAX(inspection_date::date) as latest_inspection
    FROM dohmh_daycare
    GROUP BY day_care_id
),
first_latest_inspection AS(
    SELECT
        MIN(uid) as relevant_uid
    FROM dohmh_daycare a, latest_inspections b
    WHERE a.day_care_id = b.day_care_id
    AND a.inspection_date::date = b.latest_inspection
    GROUP BY a.day_care_id
),
_dohmh_daycare_tmp AS(
    SELECT
        uid,
        source,
        REGEXP_REPLACE(
            (CASE
                WHEN center_name LIKE '%SBCC%' THEN initcap(legal_name)
                WHEN center_name LIKE '%SCHOOL BASED CHILD CARE%' THEN initcap(legal_name)
                ELSE initcap(center_name)
            END), '\x1a', ''''
        ) as facname,
        building as addressnum,
        street as streetname,
        building||' '||street as address,
        NULL as city,
        zipcode,
        borough as boro,
        NULL as borocode,
        building_identification_number as bin,
        NULL as bbl,
        (CASE
            WHEN (facility_type ~* 'camp') AND (program_type ~* 'All Age Camp')
                THEN 'Camp - All Age'
            WHEN (facility_type ~* 'camp') AND (program_type ~* 'School Age Camp')
                THEN 'Camp - School Age'
            WHEN (program_type ~* 'Preschool Camp')
                THEN 'Camp - Preschool Age'
            WHEN
                ((facility_type = 'GDC') AND (program_type = 'Child Care - Infants/Toddlers' OR program_type = 'INFANT TODDLER'))
                OR ((facility_type = 'GDC') AND (program_type = 'Child Care - Pre School' OR program_type = 'PRESCHOOL'))
                THEN 'Day Care'
            WHEN (facility_type = 'SBCC') AND (program_type = 'PRESCHOOL')
                THEN 'School Based Child Care - Preschool'
            WHEN (facility_type = 'SBCC') AND (program_type = 'INFANT TODDLER')
                THEN 'School Based Child Care - Infants/Toddlers'
            WHEN facility_type = 'SBCC'
                THEN 'School Based Child Care - Age Unspecified'
            WHEN facility_type = 'GDC'
                THEN 'Group Day Care - Age Unspecified'
            ELSE CONCAT(facility_type,' - ',program_type)
        END) as factype,
        (CASE
            WHEN (facility_type ~* 'CAMP' OR program_type ILIKE '%CAMP%')
                THEN 'Camps'
            ELSE 'Day Care'
        END) as facsubgrp,
        initcap(legal_name) as opname,
        'Non-public' as opabbrev,
        'NYCDOHMH' as overabbrev,
        NULL as capacity,
        NULL as captype,
        NULL as wkb_geometry,
        geo_1b,
        NULL as geo_bl,
        geo_bn
    FROM dohmh_daycare
    WHERE uid IN (SELECT relevant_uid FROM first_latest_inspection)
)
SELECT *
INTO _dohmh_daycare
FROM _dohmh_daycare_tmp
-- Only keep one record for facilities having the same legal_name, hnum, sname if factype = 'Day Care'
WHERE factype <> 'Day Care'
OR uid IN(
    SELECT
        MIN(uid) as min_uid
    FROM _dohmh_daycare_tmp
    WHERE factype = 'Day Care'
    GROUP BY opname, addressnum, streetname);

CALL append_to_facdb_base('_dohmh_daycare');
