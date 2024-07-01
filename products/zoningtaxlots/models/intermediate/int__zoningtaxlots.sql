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
        (CASE
            WHEN bbl IS NULL OR LENGTH(bbl) < 10
                THEN boroughcode || LPAD(taxblock, 5, '0') || LPAD(taxlot, 4, '0')::text
            ELSE bbl
        END) AS bbl,
        boroughcode,
        taxblock,
        taxlot,
        area
    FROM insertion
),

-- add commercialoverlay

add_comm AS (
    SELECT
        a.dtm_id,
        a.bbl,
        a.boroughcode,
        a.taxblock,
        a.taxlot,
        a.area,
        b1.overlay AS commercialoverlay1,
        b2.overlay AS commercialoverlay2
    FROM pop_bbl AS a
    LEFT JOIN commercialoverlay AS b1
        ON a.dtm_id = b1.dtm_id AND b1.row_number = 1
    LEFT JOIN commercialoverlay AS b2
        ON a.dtm_id = b2.dtm_id AND b2.row_number = 2
),

-- add specialdistrict

add_special AS (
    SELECT
        a.dtm_id,
        a.bbl,
        a.boroughcode,
        a.taxblock,
        a.taxlot,
        a.area,
        a.commercialoverlay1,
        a.commercialoverlay2,
        b.specialdistrict1,
        b.specialdistrict2,
        b.specialdistrict3
    FROM add_comm AS a
    LEFT JOIN specialpurpose AS b
        ON a.dtm_id = b.dtm_id
),


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
        (CASE
            WHEN b.perbblgeom >= 10 THEN b.lhlbl
        END) AS limitedheightdistrict
    FROM add_special AS a
    LEFT JOIN limitedheight AS b
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
        (CASE
            WHEN b.perbblgeom >= 10 THEN b.zoning_map
        END) AS zoningmapnumber
    FROM add_height AS a
    LEFT JOIN zoningmapindex AS b
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
        (CASE
            WHEN b.row_number = 2 THEN 'Y'
        END) AS zoningmapcode
    FROM add_zoningmapnumber AS a
    LEFT JOIN zoningmapindex AS b
        ON a.dtm_id = b.dtm_id AND b.row_number = 2
),

add_zoningdistricts AS (
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
        b1.zonedist AS zoningdistrict1,
        b2.zonedist AS zoningdistrict2,
        b3.zonedist AS zoningdistrict3,
        b4.zonedist AS zoningdistrict4
    FROM add_zoningmapcode AS a
    LEFT JOIN zoningdistricts AS b1
        ON a.dtm_id = b1.dtm_id AND b1.row_number = 1
    LEFT JOIN zoningdistricts AS b2
        ON a.dtm_id = b2.dtm_id AND b2.row_number = 2
    LEFT JOIN zoningdistricts AS b3
        ON a.dtm_id = b3.dtm_id AND b3.row_number = 3
    LEFT JOIN zoningdistricts AS b4
        ON a.dtm_id = b4.dtm_id AND b4.row_number = 4
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
    FROM add_zoningdistricts AS a
    LEFT JOIN inwoodrezooning AS b
        ON
            a.bbl = b.bbl
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
    FROM add_inwoodrezooning AS a
    LEFT JOIN zonechange AS b
        ON a.dtm_id = b.dtm_id
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
        (CASE
            WHEN zoningdistrict1 = 'PARK' AND zoningdistrict2 IS NULL
                THEN NULL
            ELSE zoningdistrict2
        END) AS zoningdistrict2,
        (CASE
            WHEN zoningdistrict1 = 'PARK' AND zoningdistrict2 IS NULL
                THEN NULL
            ELSE zoningdistrict3
        END) AS zoningdistrict3,
        (CASE
            WHEN zoningdistrict1 = 'PARK' AND zoningdistrict2 IS NULL
                THEN NULL
            ELSE zoningdistrict4
        END) AS zoningdistrict4,
        (CASE
            WHEN zoningdistrict1 = 'PARK' AND zoningdistrict2 IS NULL
                THEN NULL
            ELSE commercialoverlay1
        END) AS commercialoverlay1,
        (CASE
            WHEN zoningdistrict1 = 'PARK' AND zoningdistrict2 IS NULL
                THEN NULL
            ELSE commercialoverlay2
        END) AS commercialoverlay2,
        (CASE
            WHEN zoningdistrict1 = 'PARK' AND zoningdistrict2 IS NULL
                THEN NULL
            ELSE specialdistrict1
        END) AS specialdistrict1,
        (CASE
            WHEN zoningdistrict1 = 'PARK' AND zoningdistrict2 IS NULL
                THEN NULL
            ELSE specialdistrict2
        END) AS specialdistrict2,
        (CASE
            WHEN zoningdistrict1 = 'PARK' AND zoningdistrict2 IS NULL
                THEN NULL
            ELSE specialdistrict3
        END) AS specialdistrict3,
        (CASE
            WHEN zoningdistrict1 = 'PARK' AND zoningdistrict2 IS NULL
                THEN NULL
            ELSE limitedheightdistrict
        END) AS limitedheightdistrict,
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
        (CASE
            WHEN commercialoverlay1 = commercialoverlay2 THEN NULL
            ELSE commercialoverlay2
        END) AS commercialoverlay2,
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
        (COALESCE(commercialoverlay1, commercialoverlay2)) AS commercialoverlay1,
        (CASE
            WHEN commercialoverlay1 IS NULL THEN NULL
            ELSE commercialoverlay2
        END) AS commercialoverlay2,
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
        dtm_id::int4,
        boroughcode::text,
        TRUNC(taxblock::numeric)::text AS "taxblock",
        taxlot::text,
        bbl::text,
        zoningdistrict1::text,
        zoningdistrict2::text,
        zoningdistrict3::text,
        zoningdistrict4::text,
        commercialoverlay1::text,
        commercialoverlay2::text,
        specialdistrict1::text,
        specialdistrict2::text,
        specialdistrict3::text,
        limitedheightdistrict::text,
        zoningmapnumber::text,
        zoningmapcode::text,
        area::float8,
        inzonechange::text AS "inzonechange"
    FROM drop_invalid
)

SELECT * FROM export
