ALTER TABLE nysed_nonpublicenrollment DROP COLUMN IF EXISTS uid,
DROP COLUMN IF EXISTS source;
DROP TABLE IF EXISTS _nysed_activeinstitutions;
WITH merged AS (
    SELECT
        nysed_activeinstitutions.*,
        -- TODO: remove cast once new version of nysed_npe archived with ingest
        -- nysed_nonpublicenrollment.*,
        prek::numeric,
        halfk::numeric,
        fullk::numeric,
        g01::numeric,
        g02::numeric,
        g03::numeric,
        g04::numeric,
        g05::numeric,
        g06::numeric,
        uge::numeric,
        g07::numeric,
        g08::numeric,
        g09::numeric,
        g10::numeric,
        g11::numeric,
        g12::numeric,
        ugs::numeric,
        (
            prek::numeric
            + halfk::numeric
            + fullk::numeric
            + g01::numeric
            + g02::numeric
            + g03::numeric
            + g04::numeric
            + g05::numeric
            + g06::numeric
            + uge::numeric
            + g07::numeric
            + g08::numeric
            + g09::numeric
            + g10::numeric
            + g11::numeric
            + g12::numeric
            + ugs::numeric
        ) AS enrollment
    FROM nysed_activeinstitutions
    LEFT JOIN nysed_nonpublicenrollment
        ON nysed_activeinstitutions.sed_code::bigint = nysed_nonpublicenrollment.beds_code
    WHERE (
        inst_type_description = 'PUBLIC SCHOOLS'
        AND inst_sub_type_description LIKE '%GED%'
    )
    OR inst_sub_type_description LIKE '%CHARTER SCHOOL%'
    OR (
        inst_type_description <> 'PUBLIC SCHOOLS'
        AND inst_type_description <> 'NON-IMF SCHOOLS'
        AND inst_type_description <> 'GOVERNMENT AGENCIES' -- MAY ACTUALLY WANT TO USE THESE
        AND inst_type_description <> 'INDEPENDENT ORGANIZATIONS'
        AND inst_type_description <> 'LIBRARY SYSTEMS'
        AND inst_type_description <> 'LOCAL GOVERNMENTS'
        AND inst_type_description <> 'SCHOOL DISTRICTS'
        AND inst_sub_type_description <> 'PUBLIC LIBRARIES'
        AND inst_sub_type_description <> 'HISTORICAL RECORDS REPOSITORIES'
        AND inst_sub_type_description <> 'CHARTER CORPORATION'
        AND inst_sub_type_description <> 'HOME BOUND'
        AND inst_sub_type_description <> 'HOME INSTRUCTED'
        AND inst_sub_type_description <> 'NYC BUREAU'
        AND inst_sub_type_description <> 'NYC NETWORK'
        AND inst_sub_type_description <> 'OUT OF DISTRICT PLACEMENT'
        AND inst_sub_type_description <> 'BUILDINGS UNDER CONSTRUCTION'
    )
)
SELECT
    uid,
    source,
    popular_name AS facname,
    NULL AS addressnum,
    NULL AS streetname,
    physical_address_line1 AS address,
    NULL AS city,
    NULL AS zipcode,
    county_description AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    (
        CASE
            WHEN inst_sub_type_description LIKE '%CHARTER SCHOOL%' THEN 'Charter School'
            WHEN inst_type_description = 'CUNY'
                THEN concat(
                    'CUNY - ',
                    initcap(right(inst_sub_type_description, -5))
                )
            WHEN inst_type_description = 'SUNY'
                THEN concat(
                    'SUNY - ',
                    initcap(right(inst_sub_type_description, -5))
                )
            WHEN
                inst_type_description = 'NON-PUBLIC SCHOOLS'
                AND prek + halfk + fullk + g01 + g02 + g03 + g04 + g05 + uge > 0
                THEN 'Elementary School - Non-public'
            WHEN
                inst_type_description = 'NON-PUBLIC SCHOOLS'
                AND g06 + g07 + g08 > 0 THEN 'Middle School - Non-public'
            WHEN
                inst_type_description = 'NON-PUBLIC SCHOOLS'
                AND g09 + g10 + g11 + g12 + ugs > 0 THEN 'High School - Non-public'
            WHEN
                inst_type_description = 'NON-PUBLIC SCHOOLS'
                AND inst_sub_type_description NOT LIKE 'ESL'
                AND inst_sub_type_description NOT LIKE 'BUILDING' THEN 'Other School - Non-public'
            WHEN inst_sub_type_description LIKE '%AHSEP%' THEN initcap(split_part(inst_sub_type_description, '(', 1))
            ELSE initcap(inst_sub_type_description)
        END
    ) AS factype,
    (
        CASE
            WHEN inst_sub_type_description LIKE '%GED-ALTERNATIVE%' THEN 'GED and Alternative High School Equivalency'
            WHEN inst_sub_type_description LIKE '%CHARTER SCHOOL%' THEN 'Charter K-12 Schools'
            WHEN inst_sub_type_description LIKE '%MUSEUM%' THEN 'Museums'
            WHEN inst_sub_type_description LIKE '%HISTORICAL%' THEN 'Historical Societies'
            WHEN inst_type_description LIKE '%LIBRARIES%' THEN 'Academic and Special Libraries'
            WHEN inst_type_description LIKE '%CHILD NUTRITION%' THEN 'Child Nutrition'
            WHEN
                inst_sub_type_description LIKE '%PRE-SCHOOL%'
                AND (
                    inst_sub_type_description LIKE '%DISABILITIES%'
                    OR inst_sub_type_description LIKE '%SWD%'
                ) THEN 'Preschools for Students with Disabilities'
            WHEN (inst_type_description LIKE '%DISABILITIES%') THEN 'Public and Private Special Education Schools'
            WHEN inst_sub_type_description LIKE '%PRE-K%' THEN 'City Government Offices'
            WHEN
                (inst_type_description LIKE 'PUBLIC%')
                OR (inst_sub_type_description LIKE 'PUBLIC%') THEN 'Public K-12 Schools'
            WHEN
                (inst_type_description LIKE '%COLLEGE%')
                OR (inst_type_description LIKE '%CUNY%')
                OR (inst_type_description LIKE '%SUNY%')
                OR (inst_type_description LIKE '%SUNY%') THEN 'Colleges or Universities'
            WHEN inst_type_description LIKE '%PROPRIETARY%' THEN 'Proprietary Schools'
            WHEN inst_type_description LIKE '%NON-IMF%' THEN 'Public K-12 Schools'
            ELSE 'Non-Public K-12 Schools'
        END
    ) AS facsubgrp,
    (
        CASE
            WHEN inst_type_description = 'PUBLIC SCHOOLS' THEN 'NYC Department of Education'
            WHEN inst_type_description LIKE '%NON-IMF%' THEN 'NYC Department of Education'
            WHEN inst_type_description = 'CUNY' THEN 'City University of New York'
            WHEN inst_type_description = 'SUNY' THEN 'State University of New York'
            ELSE initcap(legal_name)
        END
    ) AS opname,
    (
        CASE
            WHEN inst_type_description = 'PUBLIC SCHOOLS' THEN 'NYCDOE'
            WHEN inst_type_description = 'CUNY' THEN 'CUNY'
            WHEN inst_type_description = 'SUNY' THEN 'SUNY'
            ELSE 'Non-public'
        END
    ) AS opabbrev,
    'NYCDOE' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    st_point(
        gis_longitute_x::double precision,
        gis_latitude_y::double precision
    ) AS wkb_geometry,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _nysed_activeinstitutions
FROM merged;

CALL append_to_facdb_base('_nysed_activeinstitutions');
