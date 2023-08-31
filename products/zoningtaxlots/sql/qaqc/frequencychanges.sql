CREATE TEMP TABLE qaqc_frequency_change AS
WITH count_old AS (
    SELECT
        b.key AS field,
        b.value::numeric AS count
    FROM (
        SELECT row_to_json(row) AS _col
        FROM (
            SELECT
                sum((zoningdistrict1 IS NOT null)::int) AS zoningdistrict1,
                sum((zoningdistrict2 IS NOT null)::int) AS zoningdistrict2,
                sum((zoningdistrict3 IS NOT null)::int) AS zoningdistrict3,
                sum((zoningdistrict4 IS NOT null)::int) AS zoningdistrict4,
                sum((commercialoverlay1 IS NOT null)::int) AS commercialoverlay1,
                sum((commercialoverlay2 IS NOT null)::int) AS commercialoverlay2,
                sum((specialdistrict1 IS NOT null)::int) AS specialdistrict1,
                sum((specialdistrict2 IS NOT null)::int) AS specialdistrict2,
                sum((specialdistrict3 IS NOT null)::int) AS specialdistrict3,
                sum((limitedheightdistrict IS NOT null)::int) AS limitedheightdistrict,
                sum((zoningmapnumber IS NOT null)::int) AS zoningmapnumber,
                sum((zoningmapcode IS NOT null)::int) AS zoningmapcode
            FROM dcp_zoningtaxlots.:"VERSION_PREV"
        ) AS row
    ) AS a, json_each_text(_col) AS b
),

count_new AS (
    SELECT
        b.key AS field,
        b.value::numeric AS count
    FROM (
        SELECT row_to_json(row) AS _col
        FROM (
            SELECT
                sum((zoningdistrict1 IS NOT null)::int) AS zoningdistrict1,
                sum((zoningdistrict2 IS NOT null)::int) AS zoningdistrict2,
                sum((zoningdistrict3 IS NOT null)::int) AS zoningdistrict3,
                sum((zoningdistrict4 IS NOT null)::int) AS zoningdistrict4,
                sum((commercialoverlay1 IS NOT null)::int) AS commercialoverlay1,
                sum((commercialoverlay2 IS NOT null)::int) AS commercialoverlay2,
                sum((specialdistrict1 IS NOT null)::int) AS specialdistrict1,
                sum((specialdistrict2 IS NOT null)::int) AS specialdistrict2,
                sum((specialdistrict3 IS NOT null)::int) AS specialdistrict3,
                sum((limitedheightdistrict IS NOT null)::int) AS limitedheightdistrict,
                sum((zoningmapnumber IS NOT null)::int) AS zoningmapnumber,
                sum((zoningmapcode IS NOT null)::int) AS zoningmapcode
            FROM dcp_zoningtaxlots.:"VERSION"
        ) AS row
    ) AS a, json_each_text(_col) AS b
)

SELECT
    a.field,
    a.count AS countold,
    b.count AS countnew
FROM count_old AS a INNER JOIN count_new AS b ON a.field = b.field
ORDER BY b.count - a.count DESC;

\COPY qaqc_frequency_change TO PSTDOUT DELIMITER ',' CSV HEADER;
