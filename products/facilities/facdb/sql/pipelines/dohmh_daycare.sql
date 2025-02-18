DROP TABLE IF EXISTS _dohmh_daycare;
WITH
inspection_dates AS (
    SELECT
        uid,
        (CASE
            WHEN UPPER(inspection_date) = 'NONE' THEN '01/01/2000'
            ELSE inspection_date
        END) AS inspection_date
    FROM dohmh_daycare
),
latest_inspections AS (
    SELECT
        day_care_id,
        MAX(inspection_date::date) AS latest_inspection
    FROM dohmh_daycare
    GROUP BY day_care_id
),
first_latest_inspection AS (
    SELECT MIN(uid) AS relevant_uid
    FROM dohmh_daycare AS a, latest_inspections AS b
    WHERE
        a.day_care_id = b.day_care_id
        AND a.inspection_date::date = b.latest_inspection
    GROUP BY a.day_care_id
),
_dohmh_daycare_tmp AS (
    SELECT
        uid,
        source,
        REGEXP_REPLACE(
            (CASE
                WHEN center_name LIKE '%SBCC%' THEN INITCAP(legal_name)
                WHEN center_name LIKE '%SCHOOL BASED CHILD CARE%' THEN INITCAP(legal_name)
                ELSE INITCAP(center_name)
            END), '\x1a', ''''
        ) AS facname,
        building AS addressnum,
        street AS streetname,
        building || ' ' || street AS address,
        NULL AS city,
        zipcode,
        borough AS boro,
        NULL AS borocode,
        building_identification_number AS bin,
        NULL AS bbl,
        (CASE
            WHEN (facility_type ~* 'camp') AND (program_type ~* 'All Age Camp')
                THEN 'Camp - All Age'
            WHEN (facility_type ~* 'camp') AND (program_type ~* 'School Age Camp')
                THEN 'Camp - School Age'
            WHEN (program_type ~* 'Preschool Camp')
                THEN 'Camp - Preschool Age'
            WHEN
                (
                    (facility_type = 'GDC')
                    AND (program_type = 'Child Care - Infants/Toddlers' OR program_type = 'INFANT TODDLER')
                )
                OR (
                    (facility_type = 'GDC') AND (program_type = 'Child Care - Pre School' OR program_type = 'PRESCHOOL')
                )
                THEN 'Day Care'
            WHEN (facility_type = 'SBCC') AND (program_type = 'PRESCHOOL')
                THEN 'School Based Child Care - Preschool'
            WHEN (facility_type = 'SBCC') AND (program_type = 'INFANT TODDLER')
                THEN 'School Based Child Care - Infants/Toddlers'
            WHEN facility_type = 'SBCC'
                THEN 'School Based Child Care - Age Unspecified'
            WHEN facility_type = 'GDC'
                THEN 'Group Day Care - Age Unspecified'
            ELSE CONCAT(facility_type, ' - ', program_type)
        END) AS factype,
        (CASE
            WHEN (facility_type ~* 'CAMP' OR program_type ILIKE '%CAMP%')
                THEN 'Camps'
            ELSE 'Day Care'
        END) AS facsubgrp,
        INITCAP(legal_name) AS opname,
        'Non-public' AS opabbrev,
        'NYCDOHMH' AS overabbrev,
        NULL AS capacity,
        NULL AS captype,
        NULL AS wkb_geometry,
        geo_1b,
        NULL AS geo_bl,
        geo_bn
    FROM dohmh_daycare
    WHERE uid IN (SELECT relevant_uid FROM first_latest_inspection)
)
SELECT *
INTO _dohmh_daycare
FROM _dohmh_daycare_tmp
-- Only keep one record for facilities having the same legal_name, hnum, sname if factype = 'Day Care'
WHERE
    factype <> 'Day Care'
    OR uid IN (
        SELECT MIN(filtered.uid) AS min_uid
        FROM _dohmh_daycare_tmp AS filtered
        WHERE filtered.factype = 'Day Care'
        GROUP BY (filtered.opname, filtered.addressnum, filtered.streetname)
    );

CALL APPEND_TO_FACDB_BASE('_dohmh_daycare');
