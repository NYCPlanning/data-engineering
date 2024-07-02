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
        b.commercialoverlay1,
        b.commercialoverlay2
    FROM pop_bbl AS a
    LEFT JOIN commercialoverlay AS b
        ON a.dtm_id = b.dtm_id
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
        b.limitedheightdistrict
    FROM add_special AS a
    LEFT JOIN limitedheight AS b
        ON a.dtm_id = b.dtm_id
),

add_zoningmap AS (
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
        b.zoningmapnumber,
        b.zoningmapcode
    FROM add_height AS a
    LEFT JOIN zoningmapindex AS b
        ON a.dtm_id = b.dtm_id
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
        b.zoningdistrict1,
        b.zoningdistrict2,
        b.zoningdistrict3,
        b.zoningdistrict4
    FROM add_zoningmap AS a
    LEFT JOIN zoningdistricts AS b
        ON a.dtm_id = b.dtm_id
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

drop_invalid AS (
    SELECT *
    FROM park
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
