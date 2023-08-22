delete from dcp_zoningtaxlots.qaqc_mismatch
where version =: 'VERSION';

insert into dcp_zoningtaxlots.qaqc_mismatch (
    select
        count(*) as total,
        sum((a.boroughcode != b.boroughcode or (a.boroughcode is null and b.boroughcode is not null))::integer) as boroughcode,
        sum((a.taxblock != b.taxblock or (a.taxblock is null and b.taxblock is not null))::integer) as taxblock,
        sum((a.taxlot != b.taxlot or (a.taxlot is null and b.taxlot is not null))::integer) as taxlot,
        sum((a.bbl != b.bbl or (a.bbl is null and b.bbl is not null))::integer) as bbl,
        sum((a.zoningdistrict1 != b.zoningdistrict1 or (a.zoningdistrict1 is null and b.zoningdistrict1 is not null))::integer) as zoningdistrict1,
        sum((a.zoningdistrict2 != b.zoningdistrict2 or (a.zoningdistrict2 is null and b.zoningdistrict2 is not null))::integer) as zoningdistrict2,
        sum((a.zoningdistrict3 != b.zoningdistrict3 or (a.zoningdistrict3 is null and b.zoningdistrict3 is not null))::integer) as zoningdistrict3,
        sum((a.zoningdistrict4 != b.zoningdistrict4 or (a.zoningdistrict4 is null and b.zoningdistrict4 is not null))::integer) as zoningdistrict4,
        sum((a.commercialoverlay1 != b.commercialoverlay1 or (a.commercialoverlay1 is null and b.commercialoverlay1 is not null))::integer) as commercialoverlay1,
        sum((a.commercialoverlay2 != b.commercialoverlay2 or (a.commercialoverlay2 is null and b.commercialoverlay2 is not null))::integer) as commercialoverlay2,
        sum((a.specialdistrict1 != b.specialdistrict1 or (a.specialdistrict1 is null and b.specialdistrict1 is not null))::integer) as specialdistrict1,
        sum((a.specialdistrict2 != b.specialdistrict2 or (a.specialdistrict2 is null and b.specialdistrict2 is not null))::integer) as specialdistrict2,
        sum((a.specialdistrict3 != b.specialdistrict3 or (a.specialdistrict3 is null and b.specialdistrict3 is not null))::integer) as specialdistrict3,
        sum((a.limitedheightdistrict != b.limitedheightdistrict or (a.limitedheightdistrict is null and b.limitedheightdistrict is not null))::integer) as limitedheightdistrict,
        sum((a.zoningmapnumber != b.zoningmapnumber or (a.zoningmapnumber is null and b.zoningmapnumber is not null))::integer) as zoningmapnumber,
        sum((a.zoningmapcode != b.zoningmapcode or (a.zoningmapcode is null and b.zoningmapcode is not null))::integer) as zoningmapcode,
        : 'VERSION' as version,
        : 'VERSION_PREV' as version_prev
    from dcp_zoningtaxlots.:"VERSION" a
    INNER JOIN dcp_zoningtaxlots.:"VERSION_PREV" b
    ON b.bbl=a.bbl
);
