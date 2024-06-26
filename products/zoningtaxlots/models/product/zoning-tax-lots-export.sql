{{ config(
    materialized = 'table'
) }}

WITH dof_dtm AS (
    SELECT * FROM {{ ref('stg__dof_dtm') }}
),

commercialoverlay AS (
    SELECT * FROM {{ ref('int__commercialoverlay') }}
),

specialpurpose AS (
    SELECT * FROM {{ ref('int__specialpurpose') }}
),

limitedheight AS (
    SELECT * FROM {{ ref('int__limitedheight') }}
),

zoningmapindex AS (
    SELECT * FROM {{ ref('int__zoningmapindex') }}
),

zoningdistricts AS (
    SELECT * FROM {{ ref('int__zoningdistricts') }}
),

inwoodrezooning AS (
    SELECT * FROM {{ ref('int__inwoodrezooning') }}
),

zonechange AS (
    SELECT * FROM {{ ref('int__inzonechange') }}
),

-- insert bbl

insertion AS (
    SELECT
        id AS dtm_id,
        bbl,
        boro AS boroughcode,
        block AS taxblock,
        lot AS taxlot,
        ST_AREA(geom) AS area
    FROM dof_dtm
),

-- populate bbl field if it's NULL

pop_bbl AS (
    SELECT 
        dtm_id,
        (CASE WHEN bbl IS NULL OR LENGTH(bbl) < 10
        THEN boroughcode || LPAD(taxblock, 5, '0') || LPAD(taxlot, 4, '0')::text 
        ELSE bbl END) AS bbl,
        boroughcode,
        taxblock,
        taxlot,
        area
    FROM insertion
),

-- add commercialoverlay

add_comm1 AS (
    SELECT 
        a.dtm_id,
        a.bbl,
        a.boroughcode,
        a.taxblock,
        a.taxlot,
        a.area,
        b.overlay AS commercialoverlay1
    FROM pop_bbl a
    LEFT JOIN commercialoverlay b
    ON a.dtm_id = b.dtm_id AND b.row_number = 1
),

add_comm2 AS (
    SELECT 
        a.dtm_id,
        a.bbl,
        a.boroughcode,
        a.taxblock,
        a.taxlot,
        a.area,
        a.commercialoverlay1,
        b.overlay AS commercialoverlay2
    FROM add_comm1 a
    LEFT JOIN commercialoverlay b
    ON a.dtm_id = b.dtm_id AND b.row_number = 2
),

-- add specialdistrict

add_special_1 AS (
    SELECT 
        a.dtm_id,
        a.bbl,
        a.boroughcode,
        a.taxblock,
        a.taxlot,
        a.area,
        a.commercialoverlay1,
        a.commercialoverlay2,
        b.sdlbl AS specialdistrict1
    FROM add_comm2 a
    LEFT JOIN specialpurpose b
    ON a.dtm_id = b.dtm_id AND b.row_number = 1
),

add_special_2 AS (
    SELECT 
        a.dtm_id,
        a.bbl,
        a.boroughcode,
        a.taxblock,
        a.taxlot,
        a.area,
        a.commercialoverlay1,
        a.commercialoverlay2,
        a.specialdistrict1,
        b.sdlbl AS specialdistrict2
    FROM add_special_1 a
    LEFT JOIN specialpurpose b
    ON a.dtm_id = b.dtm_id AND b.row_number = 2
),

add_special_3 AS (
    SELECT 
        a.dtm_id,
        a.bbl,
        a.boroughcode,
        a.taxblock,
        a.taxlot,
        a.area,
        a.commercialoverlay1,
        a.commercialoverlay2,
        a.specialdistrict1,
        a.specialdistrict2,
        b.sdlbl AS specialdistrict3
    FROM add_special_2 a
    LEFT JOIN specialpurpose b
    ON a.dtm_id = b.dtm_id AND b.row_number = 3
),
-- set the order of specialdistrict

set_sd_order AS (
    SELECT 
        dtm_id,
        bbl,
        boroughcode,
        taxblock,
        taxlot,
        area,
        commercialoverlay1,
        commercialoverlay2,
        (CASE 
        WHEN specialdistrict1 = 'MiD' AND specialdistrict2 = 'CL' THEN 'CL'
        WHEN specialdistrict1 = 'TA' AND specialdistrict2 = 'MiD' THEN 'MiD'
        WHEN specialdistrict1 = 'TA' AND specialdistrict2 = '125th' THEN '125th'
        WHEN specialdistrict1 = 'MX-16' AND specialdistrict2 = 'EC-5' THEN 'EC-5'
        WHEN specialdistrict1 = 'MX-16' AND specialdistrict2 = 'EC-6' THEN 'EC-6'
        WHEN specialdistrict1 = 'TA' AND specialdistrict2 = 'EHC' THEN 'EHC'
        WHEN specialdistrict1 = 'G' AND specialdistrict2 = 'MX-1' THEN 'MX-1'
        ELSE specialdistrict1 END) AS specialdistrict1,
        (CASE 
        WHEN specialdistrict1 = 'MiD' AND specialdistrict2 = 'CL' THEN 'MiD'
        WHEN specialdistrict1 = 'TA' AND specialdistrict2 = 'MiD' THEN 'TA'
        WHEN specialdistrict1 = 'TA' AND specialdistrict2 = '125th' THEN 'TA'
        WHEN specialdistrict1 = 'MX-16' AND specialdistrict2 = 'EC-5' THEN 'MX-16'
        WHEN specialdistrict1 = 'MX-16' AND specialdistrict2 = 'EC-6' THEN 'MX-16'
        WHEN specialdistrict1 = 'TA' AND specialdistrict2 = 'EHC' THEN 'TA'
        WHEN specialdistrict1 = 'G' AND specialdistrict2 = 'MX-1' THEN 'G'
        ELSE specialdistrict2 END) AS specialdistrict2,
        specialdistrict3
    FROM add_special_3),

-- add limitedheight

add_height AS (
    SELECT 
        a.dtm_id,
        a.bbl,
        a.boroughcode,
        a.taxblock,
        a.taxlot,
        a.area,
        a.commercialoverlay1,
        a.commercialoverlay2,
        a.specialdistrict1,
        a.specialdistrict2,
        a.specialdistrict3,
        (CASE WHEN b.perbblgeom >= 10 THEN b.lhlbl
        ELSE null END) AS limitedheightdistrict
    FROM set_sd_order a
    LEFT JOIN limitedheight b
    ON a.dtm_id = b.dtm_id
),

add_zoningmapnumber AS (
    SELECT 
        a.dtm_id,
        a.bbl,
        a.boroughcode,
        a.taxblock,
        a.taxlot,
        a.area,
        a.commercialoverlay1,
        a.commercialoverlay2,
        a.specialdistrict1,
        a.specialdistrict2,
        a.specialdistrict3,
        a.limitedheightdistrict,
        (CASE WHEN b.perbblgeom >= 10 THEN b.zoning_map
        ELSE NULL END) AS zoningmapnumber
    FROM add_height a 
    LEFT JOIN zoningmapindex b
    ON a.dtm_id = b.dtm_id AND b.row_number = 1 
),

add_zoningmapcode AS (
    SELECT 
        a.dtm_id,
        a.bbl,
        a.boroughcode,
        a.taxblock,
        a.taxlot,
        a.area,
        a.commercialoverlay1,
        a.commercialoverlay2,
        a.specialdistrict1,
        a.specialdistrict2,
        a.specialdistrict3,
        a.limitedheightdistrict,
        a.zoningmapnumber,
        (CASE WHEN b.row_number = 2 THEN 'Y'
        ELSE NULL END) AS zoningmapcode
    FROM add_zoningmapnumber a 
    LEFT JOIN zoningmapindex b
    ON a.dtm_id = b.dtm_id AND b.row_number = 2
),

add_zoningdistricts_1 AS (
    SELECT 
        a.dtm_id,
        a.bbl,
        a.boroughcode,
        a.taxblock,
        a.taxlot,
        a.area,
        a.commercialoverlay1,
        a.commercialoverlay2,
        a.specialdistrict1,
        a.specialdistrict2,
        a.specialdistrict3,
        a.limitedheightdistrict,
        a.zoningmapnumber,
        a.zoningmapcode,
        b.zonedist AS zoningdistrict1
    FROM add_zoningmapcode a
    LEFT JOIN zoningdistricts b
    ON a.dtm_id = b.dtm_id AND b.row_number = 1
),

add_zoningdistricts_2 AS (
    SELECT 
        a.dtm_id,
        a.bbl,
        a.boroughcode,
        a.taxblock,
        a.taxlot,
        a.area,
        a.commercialoverlay1,
        a.commercialoverlay2,
        a.specialdistrict1,
        a.specialdistrict2,
        a.specialdistrict3,
        a.limitedheightdistrict,
        a.zoningmapnumber,
        a.zoningmapcode,
        a.zoningdistrict1,
        b.zonedist AS zoningdistrict2
    FROM add_zoningdistricts_1 a
    LEFT JOIN zoningdistricts b
    ON a.dtm_id = b.dtm_id AND b.row_number = 2
),

add_zoningdistricts_3 AS (
    SELECT 
        a.dtm_id,
        a.bbl,
        a.boroughcode,
        a.taxblock,
        a.taxlot,
        a.area,
        a.commercialoverlay1,
        a.commercialoverlay2,
        a.specialdistrict1,
        a.specialdistrict2,
        a.specialdistrict3,
        a.limitedheightdistrict,
        a.zoningmapnumber,
        a.zoningmapcode,
        a.zoningdistrict1,
        a.zoningdistrict2,
        b.zonedist AS zoningdistrict3
    FROM add_zoningdistricts_2 a
    LEFT JOIN zoningdistricts b
    ON a.dtm_id = b.dtm_id AND b.row_number = 3
),

add_zoningdistricts_4 AS (
    SELECT 
        a.dtm_id,
        a.bbl,
        a.boroughcode,
        a.taxblock,
        a.taxlot,
        a.area,
        a.commercialoverlay1,
        a.commercialoverlay2,
        a.specialdistrict1,
        a.specialdistrict2,
        a.specialdistrict3,
        a.limitedheightdistrict,
        a.zoningmapnumber,
        a.zoningmapcode,
        a.zoningdistrict1,
        a.zoningdistrict2,
        a.zoningdistrict3,
        b.zonedist AS zoningdistrict4
    FROM add_zoningdistricts_3 a
    LEFT JOIN zoningdistricts b
    ON a.dtm_id = b.dtm_id AND b.row_number = 4
),

add_inwoodrezooning AS (
    SELECT 
        a.dtm_id,
        a.bbl,
        a.boroughcode,
        a.taxblock,
        a.taxlot,
        a.area,
        a.commercialoverlay1,
        a.commercialoverlay2,
        a.specialdistrict1,
        a.specialdistrict2,
        a.specialdistrict3,
        a.limitedheightdistrict,
        a.zoningdistrict1,
        a.zoningdistrict2,
        a.zoningdistrict3,
        a.zoningdistrict4,
        a.zoningmapnumber,
        a.zoningmapcode,
        b.notes
    FROM add_zoningdistricts_4 a
    LEFT JOIN inwoodrezooning b
    ON a.bbl=b.bbl 
    AND a.bbl != '1022552000'
    AND a.bbl != '1022550001'
    AND a.bbl != '1021890001'
    AND a.bbl != '1021970001'
),

add_inzonechange AS (
    SELECT 
        a.dtm_id,
        a.bbl,
        a.boroughcode,
        a.taxblock,
        a.taxlot,
        a.area,
        a.commercialoverlay1,
        a.commercialoverlay2,
        a.specialdistrict1,
        a.specialdistrict2,
        a.specialdistrict3,
        a.limitedheightdistrict,
        a.zoningdistrict1,
        a.zoningdistrict2,
        a.zoningdistrict3,
        a.zoningdistrict4,
        a.zoningmapnumber,
        a.zoningmapcode,
        a.notes,
        b.inzonechange
    FROM add_inwoodrezooning a 
    LEFT JOIN zonechange b
    ON a.dtm_id=b.dtm_id
),

park AS (
    SELECT 
        dtm_id,
        bbl,
        boroughcode,
        taxblock,
        taxlot,
        area,
        notes,
        inzonechange,
        zoningdistrict1,
        (CASE WHEN zoningdistrict1 = 'PARK' AND zoningdistrict2 IS NULL
        THEN NULL ELSE zoningdistrict2 END) AS zoningdistrict2,
        (CASE WHEN zoningdistrict1 = 'PARK' AND zoningdistrict2 IS NULL
        THEN NULL ELSE zoningdistrict3 END) AS zoningdistrict3,
        (CASE WHEN zoningdistrict1 = 'PARK' AND zoningdistrict2 IS NULL
        THEN NULL ELSE zoningdistrict4 END) AS zoningdistrict4,
        (CASE WHEN zoningdistrict1 = 'PARK' AND zoningdistrict2 IS NULL
        THEN NULL ELSE commercialoverlay1 END) AS commercialoverlay1,
        (CASE WHEN zoningdistrict1 = 'PARK' AND zoningdistrict2 IS NULL
        THEN NULL ELSE commercialoverlay2 END) AS commercialoverlay2,
        (CASE WHEN zoningdistrict1 = 'PARK' AND zoningdistrict2 IS NULL
        THEN NULL ELSE specialdistrict1 END) AS specialdistrict1,
        (CASE WHEN zoningdistrict1 = 'PARK' AND zoningdistrict2 IS NULL
        THEN NULL ELSE specialdistrict2 END) AS specialdistrict2,
        (CASE WHEN zoningdistrict1 = 'PARK' AND zoningdistrict2 IS NULL
        THEN NULL ELSE specialdistrict3 END) AS specialdistrict3,
        (CASE WHEN zoningdistrict1 = 'PARK' AND zoningdistrict2 IS NULL
        THEN NULL ELSE limitedheightdistrict END) AS limitedheightdistrict,
        zoningmapnumber,
        zoningmapcode
    FROM add_inzonechange
),

drop_dup AS (
    SELECT 
        dtm_id,
        bbl,
        boroughcode,
        taxblock,
        taxlot,
        area,
        notes,
        inzonechange,
        zoningdistrict1,
        zoningdistrict2,
        zoningdistrict3,
        zoningdistrict4,
        commercialoverlay1,
        (CASE WHEN commercialoverlay1 = commercialoverlay2 THEN NULL 
        ELSE commercialoverlay2 END) AS commercialoverlay2,
        specialdistrict1,
        specialdistrict2,
        specialdistrict3,
        limitedheightdistrict,
        zoningmapnumber,
        zoningmapcode
    FROM park
),

corr_zoninggaps AS (
    SELECT 
        dtm_id,
        bbl,
        boroughcode,
        taxblock,
        taxlot,
        area,
        notes,
        inzonechange,
        zoningdistrict1,
        zoningdistrict2,
        zoningdistrict3,
        zoningdistrict4,
        (CASE WHEN commercialoverlay1 IS NULL THEN commercialoverlay2 
        ELSE commercialoverlay1 END) AS commercialoverlay1,
        (CASE WHEN commercialoverlay1 IS NULL THEN NULL 
        ELSE commercialoverlay2 END) AS commercialoverlay2,
        specialdistrict1,
        specialdistrict2,
        specialdistrict3,
        limitedheightdistrict,
        zoningmapnumber,
        zoningmapcode
    FROM drop_dup
),

drop_invalid AS (
    SELECT * 
    FROM corr_zoninggaps
    WHERE 
        (boroughcode IS NOT NULL OR boroughcode != '0')
        OR (taxblock IS NOT NULL OR taxblock != '0')
        OR (taxlot IS NOT NULL OR taxlot != '0')
),

export AS (
    SELECT 
        dtm_id,
        boroughcode AS "Borough Code",
        trunc(taxblock::numeric) AS "Tax Block",
        taxlot AS "Tax Lot",
        bbl AS "BBL",
        zoningdistrict1 AS "Zoning District 1",
        zoningdistrict2 AS "Zoning District 2",
        zoningdistrict3 AS "Zoning District 3",
        zoningdistrict4 AS "Zoning District 4",
        commercialoverlay1 AS "Commercial Overlay 1",
        commercialoverlay2 AS "Commercial Overlay 2",
        specialdistrict1 AS "Special District 1",
        specialdistrict2 AS "Special District 2",
        specialdistrict3 AS "Special District 3",
        limitedheightdistrict AS "Limited Height District",
        zoningmapnumber AS "Zoning Map Number",
        zoningmapcode AS "Zoning Map Code"
    FROM drop_invalid
)

SELECT * FROM export 
