{{ config(
    materialized = 'table',
    indexes=[
        {'columns': ['bbl']},
    ]
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

lots_with_features AS (
    SELECT
        a.dtm_id,
        CASE
            WHEN a.bbl IS NULL OR length(a.bbl) < 10
                THEN a.boro || lpad(a.block, 5, '0') || lpad(a.lot, 4, '0')::text
            ELSE a.bbl
        END AS bbl,
        a.boro AS boroughcode,
        a.block AS taxblock,
        a.lot AS taxlot,
        a.geom,
        ST_Area(a.geom) AS area,
        b1.overlay AS commercialoverlay1,
        b2.overlay AS commercialoverlay2,
        c1.sdlbl AS specialdistrict1,
        c2.sdlbl AS specialdistrict2,
        c3.sdlbl AS specialdistrict3,
        d.limitedheightdistrict,
        CASE
            WHEN e1.perbblgeom >= 10 THEN e1.zoning_map
        END AS zoningmapnumber,
        CASE
            WHEN e2.row_number = 2 THEN 'Y'
        END AS zoningmapcode,
        f1.zonedist AS zoningdistrict1,
        f2.zonedist AS zoningdistrict2,
        f3.zonedist AS zoningdistrict3,
        f4.zonedist AS zoningdistrict4,
        g.notes,
        h.inzonechange
    FROM dof_dtm AS a
    LEFT JOIN commercialoverlay AS b1
        ON a.dtm_id = b1.dtm_id AND b1.row_number = 1
    LEFT JOIN commercialoverlay AS b2
        ON a.dtm_id = b2.dtm_id AND b2.row_number = 2
    LEFT JOIN specialpurpose AS c1
        ON a.dtm_id = c1.dtm_id AND c1.row_number = 1
    LEFT JOIN specialpurpose AS c2
        ON a.dtm_id = c2.dtm_id AND c2.row_number = 2
    LEFT JOIN specialpurpose AS c3
        ON a.dtm_id = c3.dtm_id AND c3.row_number = 3
    LEFT JOIN limitedheight AS d
        ON a.dtm_id = d.dtm_id
    LEFT JOIN zoningmapindex AS e1
        ON a.dtm_id = e1.dtm_id AND e1.row_number = 1
    LEFT JOIN zoningmapindex AS e2
        ON a.dtm_id = e2.dtm_id AND e2.row_number = 2
    LEFT JOIN zoningdistricts AS f1
        ON a.dtm_id = f1.dtm_id AND f1.row_number = 1
    LEFT JOIN zoningdistricts AS f2
        ON a.dtm_id = f2.dtm_id AND f2.row_number = 2
    LEFT JOIN zoningdistricts AS f3
        ON a.dtm_id = f3.dtm_id AND f3.row_number = 3
    LEFT JOIN zoningdistricts AS f4
        ON a.dtm_id = f4.dtm_id AND f4.row_number = 4
    LEFT JOIN inwoodrezooning AS g
        ON a.dtm_id = g.dtm_id
    LEFT JOIN zonechange AS h
        ON a.dtm_id = h.dtm_id
),

clean_park_features AS (
    SELECT
        dtm_id,
        bbl,
        boroughcode,
        taxblock,
        taxlot,
        geom,
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
    FROM lots_with_features
),

drop_invalid AS (
    SELECT *
    FROM clean_park_features
    WHERE
        (boroughcode IS NOT NULL OR boroughcode != '0')
        OR (taxblock IS NOT NULL OR taxblock != '0')
        OR (taxlot IS NOT NULL OR taxlot != '0')
)

SELECT
    dtm_id::int4,
    boroughcode::text AS borough_code,
    trunc(taxblock::numeric)::text AS tax_block,
    taxlot::text AS tax_lot,
    bbl::text,
    zoningdistrict1::text AS zoning_district_1,
    zoningdistrict2::text AS zoning_district_2,
    zoningdistrict3::text AS zoning_district_3,
    zoningdistrict4::text AS zoning_district_4,
    commercialoverlay1::text AS commercial_overlay_1,
    commercialoverlay2::text AS commercial_overlay_2,
    specialdistrict1::text AS special_district_1,
    specialdistrict2::text AS special_district_2,
    specialdistrict3::text AS special_district_3,
    limitedheightdistrict::text AS limited_height_district,
    zoningmapnumber::text AS zoning_map_number,
    zoningmapcode::text AS zoning_map_code,
    area::float8,
    inzonechange::text,
    geom::geometry
FROM drop_invalid
