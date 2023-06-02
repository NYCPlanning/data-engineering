CREATE TEMP TABLE qaqc_frequency_change AS (
    WITH
    count_old as (
        SELECT b.key as field, b.value::numeric as count
        FROM( SELECT row_to_json(row) as _col 
        FROM(
        SELECT 
            sum(zoningdistrict1 is not null::int) as zoningdistrict1,
            sum(zoningdistrict2 is not null::int) as zoningdistrict2,
            sum(zoningdistrict3 is not null::int) as zoningdistrict3,
            sum(zoningdistrict4 is not null::int) as zoningdistrict4,
            sum(commercialoverlay1 is not null::int) as commercialoverlay1,
            sum(commercialoverlay2 is not null::int) as commercialoverlay2,
            sum(specialdistrict1 is not null::int) as specialdistrict1,
            sum(specialdistrict2 is not null::int) as specialdistrict2,
            sum(specialdistrict3 is not null::int) as specialdistrict3,
            sum(limitedheightdistrict is not null::int) as limitedheightdistrict,
            sum(zoningmapnumber is not null::int) as zoningmapnumber,
            sum(zoningmapcode is not null::int) as zoningmapcode
        FROM dcp_zoningtaxlots.:"VERSION_PREV") row) a, json_each_text(_col) as b
    ),
    count_new as (
        SELECT b.key as field, b.value::numeric as count
        FROM( SELECT row_to_json(row) as _col 
        FROM(
        SELECT 
            sum(zoningdistrict1 is not null::int) as zoningdistrict1,
            sum(zoningdistrict2 is not null::int) as zoningdistrict2,
            sum(zoningdistrict3 is not null::int) as zoningdistrict3,
            sum(zoningdistrict4 is not null::int) as zoningdistrict4,
            sum(commercialoverlay1 is not null::int) as commercialoverlay1,
            sum(commercialoverlay2 is not null::int) as commercialoverlay2,
            sum(specialdistrict1 is not null::int) as specialdistrict1,
            sum(specialdistrict2 is not null::int) as specialdistrict2,
            sum(specialdistrict3 is not null::int) as specialdistrict3,
            sum(limitedheightdistrict is not null::int) as limitedheightdistrict,
            sum(zoningmapnumber is not null::int) as zoningmapnumber,
            sum(zoningmapcode is not null::int) as zoningmapcode
        FROM dcp_zoningtaxlots.:"VERSION") row) a, json_each_text(_col) as b
    ) 
    SELECT a.field, a.count as countold, b.count as countnew
    FROM count_old a JOIN count_new b on a.field=b.field
    ORDER BY b.count - a.count DESC       
);

\COPY qaqc_frequency_change TO PSTDOUT DELIMITER ',' CSV HEADER;