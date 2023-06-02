delete from dcp_zoningtaxlots.qaqc_mismatch
where version = :'VERSION';

insert into dcp_zoningtaxlots.qaqc_mismatch (
    SELECT
    count(*) as total,
    sum((a.boroughcode <> b.boroughcode or (a.boroughcode IS NULL AND b.boroughcode IS NOT NULL))::integer) as boroughcode,
    sum((a.taxblock <>  b.taxblock or (a.taxblock IS NULL AND b.taxblock IS NOT NULL))::integer)  as taxblock,
    sum((a.taxlot <>  b.taxlot or (a.taxlot IS NULL AND b.taxlot IS NOT NULL))::integer)  as taxlot,
    sum((a.bbl <> b.bbl or (a.bbl IS NULL AND b.bbl IS NOT NULL))::integer)  as bbl,
    sum((a.zoningdistrict1 <> b.zoningdistrict1 or (a.zoningdistrict1 IS NULL AND b.zoningdistrict1 IS NOT NULL))::integer)  as zoningdistrict1,
    sum((a.zoningdistrict2 <> b.zoningdistrict2 or (a.zoningdistrict2 IS NULL AND b.zoningdistrict2 IS NOT NULL))::integer)  as zoningdistrict2,
    sum((a.zoningdistrict3 <> b.zoningdistrict3 or (a.zoningdistrict3 IS NULL AND b.zoningdistrict3 IS NOT NULL))::integer)  as zoningdistrict3,
    sum((a.zoningdistrict4 <> b.zoningdistrict4 or (a.zoningdistrict4 IS NULL AND b.zoningdistrict4 IS NOT NULL))::integer)  as zoningdistrict4,
    sum((a.commercialoverlay1 <> b.commercialoverlay1 or (a.commercialoverlay1 IS NULL AND b.commercialoverlay1 IS NOT NULL))::integer)  as commercialoverlay1,
    sum((a.commercialoverlay2 <> b.commercialoverlay2 or( a.commercialoverlay2 IS NULL AND b.commercialoverlay2 IS NOT NULL))::integer)  as commercialoverlay2,
    sum((a.specialdistrict1 <> b.specialdistrict1 or (a.specialdistrict1 IS NULL AND b.specialdistrict1 IS NOT NULL))::integer)  as specialdistrict1,
    sum((a.specialdistrict2 <> b.specialdistrict2 or (a.specialdistrict2 IS NULL AND b.specialdistrict2 IS NOT NULL))::integer)  as specialdistrict2,
    sum((a.specialdistrict3 <> b.specialdistrict3 or (a.specialdistrict3 IS NULL AND b.specialdistrict3 IS NOT NULL))::integer)  as specialdistrict3,
    sum((a.limitedheightdistrict <> b.limitedheightdistrict or (a.limitedheightdistrict IS NULL AND b.limitedheightdistrict IS NOT NULL))::integer)  as limitedheightdistrict,
    sum((a.zoningmapnumber <> b.zoningmapnumber or (a.zoningmapnumber IS NULL AND b.zoningmapnumber IS NOT NULL))::integer)  as zoningmapnumber,
    sum((a.zoningmapcode <> b.zoningmapcode or (a.zoningmapcode IS NULL AND b.zoningmapcode IS NOT NULL))::integer)  as zoningmapcode,
    :'VERSION' as version,
    :'VERSION_PREV' as version_prev
    FROM dcp_zoningtaxlots.:"VERSION" a
    INNER JOIN dcp_zoningtaxlots.:"VERSION_PREV" b
    ON b.bbl=a.bbl
);