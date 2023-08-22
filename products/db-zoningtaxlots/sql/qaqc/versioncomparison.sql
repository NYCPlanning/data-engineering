CREATE TEMP TABLE qaqc_field_change AS (
    WITH pivot_mismatch AS (
        SELECT
            'zoningdistrict1' AS field,
            count(nullif(a.zoningdistrict1 = b.zoningdistrict1, true)) AS count
        FROM dcp_zoningtaxlots.:"VERSION" a
            INNER JOIN dcp_zoningtaxlots.:"VERSION_PREV" b
            ON b.bbl=a.bbl
            WHERE a.zoningdistrict1 IS NOT NULL
        UNION
        SELECT
            'zoningdistrict2' AS field,
            count(nullif(a.zoningdistrict2 = b.zoningdistrict2, true)) AS count
        FROM dcp_zoningtaxlots.:"VERSION" a
            INNER JOIN dcp_zoningtaxlots.:"VERSION_PREV" b
            ON b.bbl=a.bbl
            WHERE a.zoningdistrict2 IS NOT NULL
        UNION
        SELECT
            'zoningdistrict3' AS field,
            count(nullif(a.zoningdistrict3 = b.zoningdistrict3, true)) AS count
        FROM dcp_zoningtaxlots.:"VERSION" a
            INNER JOIN dcp_zoningtaxlots.:"VERSION_PREV" b
            ON b.bbl=a.bbl
            WHERE a.zoningdistrict3 IS NOT NULL
        UNION
        SELECT
            'zoningdistrict4' AS field,
            count(nullif(a.zoningdistrict4 = b.zoningdistrict4, true)) AS count
        FROM dcp_zoningtaxlots.:"VERSION" a
            INNER JOIN dcp_zoningtaxlots.:"VERSION_PREV" b
            ON b.bbl=a.bbl
            WHERE a.zoningdistrict4 IS NOT NULL
        UNION
        SELECT
            'commercialoverlay1' AS field,
            count(nullif(a.commercialoverlay1 = b.commercialoverlay1, true)) AS count
        FROM dcp_zoningtaxlots.:"VERSION" a
            INNER JOIN dcp_zoningtaxlots.:"VERSION_PREV" b
            ON b.bbl=a.bbl
            WHERE a.commercialoverlay1 IS NOT NULL
        UNION
        SELECT
            'commercialoverlay2' AS field,
            count(nullif(a.commercialoverlay2 = b.commercialoverlay2, true)) AS count
        FROM dcp_zoningtaxlots.:"VERSION" a
            INNER JOIN dcp_zoningtaxlots.:"VERSION_PREV" b
            ON b.bbl=a.bbl
            WHERE a.commercialoverlay2 IS NOT NULL
        UNION
        SELECT
            'specialdistrict1' AS field,
            count(nullif(a.specialdistrict1 = b.specialdistrict1, true)) AS count
        FROM dcp_zoningtaxlots.:"VERSION" a
            INNER JOIN dcp_zoningtaxlots.:"VERSION_PREV" b
            ON b.bbl=a.bbl
            WHERE a.specialdistrict1 IS NOT NULL
        UNION
        SELECT
            'specialdistrict2' AS field,
            count(nullif(a.specialdistrict2 = b.specialdistrict2, true)) AS count
        FROM dcp_zoningtaxlots.:"VERSION" a
            INNER JOIN dcp_zoningtaxlots.:"VERSION_PREV" b
            ON b.bbl=a.bbl
            WHERE a.specialdistrict2 IS NOT NULL
        UNION
        SELECT
            'specialdistrict3' AS field,
            count(nullif(a.specialdistrict3 = b.specialdistrict3, true)) AS count
        FROM dcp_zoningtaxlots.:"VERSION" a
            INNER JOIN dcp_zoningtaxlots.:"VERSION_PREV" b
            ON b.bbl=a.bbl
            WHERE a.specialdistrict3 IS NOT NULL
        UNION
        SELECT
            'limitedheightdistrict' AS field,
            count(nullif(a.limitedheightdistrict = b.limitedheightdistrict, true)) AS count
        FROM dcp_zoningtaxlots.:"VERSION" a
            INNER JOIN dcp_zoningtaxlots.:"VERSION_PREV" b
            ON b.bbl=a.bbl
            WHERE a.limitedheightdistrict IS NOT NULL
        UNION
        SELECT
            'zoningmapnumber' AS field,
            count(nullif(a.zoningmapnumber = b.zoningmapnumber, true)) AS count
        FROM dcp_zoningtaxlots.:"VERSION" a
            INNER JOIN dcp_zoningtaxlots.:"VERSION_PREV" b
            ON b.bbl=a.bbl
            WHERE a.zoningmapnumber IS NOT NULL
        UNION
        SELECT
            'zoningmapcode' AS field,
            count(nullif(a.zoningmapcode = b.zoningmapcode, true)) AS count
        FROM dcp_zoningtaxlots.:"VERSION" a
            INNER JOIN dcp_zoningtaxlots.:"VERSION_PREV" b
            ON b.bbl=a.bbl
            WHERE a.zoningmapcode IS NOT NULL
    ),

    countall AS (
        SELECT count(*) AS countall
        FROM dcp_zoningtaxlots.:"VERSION" a
        INNER JOIN dcp_zoningtaxlots.:"VERSION_PREV" b
        ON b.bbl=a.bbl
    )

    SELECT
        a.field,
        a.count AS count,
        round((a.count / b.countall) * 100, 2) AS percent
    FROM pivot_mismatch AS a, countall AS b
    ORDER BY percent DESC
);

\COPY qaqc_field_change TO PSTDOUT DELIMITER ',' CSV HEADER;
