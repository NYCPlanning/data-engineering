DELETE FROM dcp_zoningtaxlots.qaqc_mismatch
WHERE version = :'VERSION';

INSERT INTO dcp_zoningtaxlots.qaqc_mismatch (
    SELECT
        count(*) AS total,
        sum(
            (a.boroughcode != b.boroughcode OR (a.boroughcode IS NULL AND b.boroughcode IS NOT NULL))::integer
        ) AS boroughcode,
        sum((a.taxblock != b.taxblock OR (a.taxblock IS NULL AND b.taxblock IS NOT NULL))::integer) AS taxblock,
        sum((a.taxlot != b.taxlot OR (a.taxlot IS NULL AND b.taxlot IS NOT NULL))::integer) AS taxlot,
        sum((a.bbl != b.bbl OR (a.bbl IS NULL AND b.bbl IS NOT NULL))::integer) AS bbl,
        sum(
            (
                a.zoningdistrict1 != b.zoningdistrict1 OR (a.zoningdistrict1 IS NULL AND b.zoningdistrict1 IS NOT NULL)
            )::integer
        ) AS zoningdistrict1,
        sum(
            (
                a.zoningdistrict2 != b.zoningdistrict2 OR (a.zoningdistrict2 IS NULL AND b.zoningdistrict2 IS NOT NULL)
            )::integer
        ) AS zoningdistrict2,
        sum(
            (
                a.zoningdistrict3 != b.zoningdistrict3 OR (a.zoningdistrict3 IS NULL AND b.zoningdistrict3 IS NOT NULL)
            )::integer
        ) AS zoningdistrict3,
        sum(
            (
                a.zoningdistrict4 != b.zoningdistrict4 OR (a.zoningdistrict4 IS NULL AND b.zoningdistrict4 IS NOT NULL)
            )::integer
        ) AS zoningdistrict4,
        sum(
            (
                a.commercialoverlay1 != b.commercialoverlay1
                OR (a.commercialoverlay1 IS NULL AND b.commercialoverlay1 IS NOT NULL)
            )::integer
        ) AS commercialoverlay1,
        sum(
            (
                a.commercialoverlay2 != b.commercialoverlay2
                OR (a.commercialoverlay2 IS NULL AND b.commercialoverlay2 IS NOT NULL)
            )::integer
        ) AS commercialoverlay2,
        sum(
            (
                a.specialdistrict1 != b.specialdistrict1
                OR (a.specialdistrict1 IS NULL AND b.specialdistrict1 IS NOT NULL)
            )::integer
        ) AS specialdistrict1,
        sum(
            (
                a.specialdistrict2 != b.specialdistrict2
                OR (a.specialdistrict2 IS NULL AND b.specialdistrict2 IS NOT NULL)
            )::integer
        ) AS specialdistrict2,
        sum(
            (
                a.specialdistrict3 != b.specialdistrict3
                OR (a.specialdistrict3 IS NULL AND b.specialdistrict3 IS NOT NULL)
            )::integer
        ) AS specialdistrict3,
        sum(
            (
                a.limitedheightdistrict != b.limitedheightdistrict
                OR (a.limitedheightdistrict IS NULL AND b.limitedheightdistrict IS NOT NULL)
            )::integer
        ) AS limitedheightdistrict,
        sum(
            (
                a.zoningmapnumber != b.zoningmapnumber OR (a.zoningmapnumber IS NULL AND b.zoningmapnumber IS NOT NULL)
            )::integer
        ) AS zoningmapnumber,
        sum(
            (a.zoningmapcode != b.zoningmapcode OR (a.zoningmapcode IS NULL AND b.zoningmapcode IS NOT NULL))::integer
        ) AS zoningmapcode,
        :'VERSION' AS version,
        :'VERSION_PREV' AS version_prev
    FROM dcp_zoningtaxlots.:"VERSION" AS a
    INNER JOIN dcp_zoningtaxlots.:"VERSION_PREV" AS b
        ON a.bbl = b.bbl
);
