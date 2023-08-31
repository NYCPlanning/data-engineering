-- output a diff file with bbls that have changed in any field
CREATE TEMP TABLE bbldiffs AS (
    SELECT
        a.boroughcode,
        a.taxblock,
        a.taxlot,
        a.bbl AS bblnew,
        a.zoningdistrict1 AS zd1new,
        a.zoningdistrict2 AS zd2new,
        a.zoningdistrict3 AS zd3new,
        a.zoningdistrict4 AS zd4new,
        a.commercialoverlay1 AS co1new,
        a.commercialoverlay2 AS co2new,
        a.specialdistrict1 AS sd1new,
        a.specialdistrict2 AS sd2new,
        a.specialdistrict3 AS sd3new,
        a.limitedheightdistrict AS lhdnew,
        a.zoningmapnumber AS zmnnew,
        a.zoningmapcode AS zmcnew,
        a.area,
        a.inzonechange,
        b.bbl AS bblprev,
        b.zoningdistrict1 AS zd1prev,
        b.zoningdistrict2 AS zd2prev,
        b.zoningdistrict3 AS zd3prev,
        b.zoningdistrict4 AS zd4prev,
        b.commercialoverlay1 AS co1prev,
        b.commercialoverlay2 AS co2prev,
        b.specialdistrict1 AS sd1prev,
        b.specialdistrict2 AS sd2prev,
        b.specialdistrict3 AS sd3prev,
        b.limitedheightdistrict AS lhdprev,
        b.zoningmapnumber AS zmnprev,
        b.zoningmapcode AS zmcprev
    FROM dcp_zoningtaxlots.:"VERSION" AS a, dcp_zoningtaxlots.:"VERSION_PREV" AS b
    WHERE
        a.bbl::text = b.bbl::text
        AND (
            a.zoningdistrict1 != b.zoningdistrict1
            OR a.zoningdistrict2 != b.zoningdistrict2
            OR a.zoningdistrict3 != b.zoningdistrict3
            OR a.zoningdistrict4 != b.zoningdistrict4
            OR a.commercialoverlay1 != b.commercialoverlay1
            OR a.commercialoverlay2 != b.commercialoverlay2
            OR a.specialdistrict1 != b.specialdistrict1
            OR a.specialdistrict2 != b.specialdistrict2
            OR a.specialdistrict3 != b.specialdistrict3
            OR a.limitedheightdistrict != b.limitedheightdistrict
            OR a.zoningmapnumber != b.zoningmapnumber
            OR a.zoningmapcode != b.zoningmapcode
            OR a.zoningdistrict1 IS NULL AND b.zoningdistrict1 IS NOT NULL
            OR a.zoningdistrict2 IS NULL AND b.zoningdistrict2 IS NOT NULL
            OR a.zoningdistrict3 IS NULL AND b.zoningdistrict3 IS NOT NULL
            OR a.zoningdistrict4 IS NULL AND b.zoningdistrict4 IS NOT NULL
            OR a.commercialoverlay1 IS NULL AND b.commercialoverlay1 IS NOT NULL
            OR a.commercialoverlay2 IS NULL AND b.commercialoverlay2 IS NOT NULL
            OR a.specialdistrict1 IS NULL AND b.specialdistrict1 IS NOT NULL
            OR a.specialdistrict2 IS NULL AND b.specialdistrict2 IS NOT NULL
            OR a.specialdistrict3 IS NULL AND b.specialdistrict3 IS NOT NULL
            OR a.zoningmapnumber IS NULL AND b.zoningmapnumber IS NOT NULL
            OR a.zoningmapcode IS NULL AND b.zoningmapcode IS NOT NULL
            OR b.zoningdistrict1 IS NULL AND a.zoningdistrict1 IS NOT NULL
            OR b.zoningdistrict2 IS NULL AND a.zoningdistrict2 IS NOT NULL
            OR b.zoningdistrict3 IS NULL AND a.zoningdistrict3 IS NOT NULL
            OR b.zoningdistrict4 IS NULL AND a.zoningdistrict4 IS NOT NULL
            OR b.commercialoverlay1 IS NULL AND a.commercialoverlay1 IS NOT NULL
            OR b.commercialoverlay2 IS NULL AND a.commercialoverlay2 IS NOT NULL
            OR b.specialdistrict1 IS NULL AND a.specialdistrict1 IS NOT NULL
            OR b.specialdistrict2 IS NULL AND a.specialdistrict2 IS NOT NULL
            OR b.specialdistrict3 IS NULL AND a.specialdistrict3 IS NOT NULL
            OR b.zoningmapnumber IS NULL AND a.zoningmapnumber IS NOT NULL
            OR b.zoningmapcode IS NULL AND a.zoningmapcode IS NOT NULL
        )
);

\COPY bbldiffs TO PSTDOUT DELIMITER ',' CSV HEADER;
