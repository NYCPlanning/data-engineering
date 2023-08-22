delete from dcp_zoningtaxlots.qaqc_frequency
where version =: 'VERSION';

insert into dcp_zoningtaxlots.qaqc_frequency (
    select
        count(boroughcode) as boroughcode,
        count(taxblock) as taxblock,
        count(taxlot) as taxlot,
        count(bbl) as bbl,
        count(zoningdistrict1) as zoningdistrict1,
        count(zoningdistrict2) as zoningdistrict2,
        count(zoningdistrict3) as zoningdistrict3,
        count(zoningdistrict4) as zoningdistrict4,
        count(commercialoverlay1) as commercialoverlay1,
        count(commercialoverlay2) as commercialoverlay2,
        count(specialdistrict1) as specialdistrict1,
        count(specialdistrict2) as specialdistrict2,
        count(specialdistrict3) as specialdistrict3,
        count(limitedheightdistrict) as limitedheightdistrict,
        count(mihflag) as mihflag,
        count(mihoption) as mihoption,
        count(zoningmapnumber) as zoningmapnumber,
        count(zoningmapcode) as zoningmapcode,
        count(area) as area,
        : 'VERSION' as version
    from dcp_zoningtaxlots.:"VERSION"
);
