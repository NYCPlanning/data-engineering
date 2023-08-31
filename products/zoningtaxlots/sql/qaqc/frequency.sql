DELETE FROM dcp_zoningtaxlots.qaqc_frequency
WHERE version = :'VERSION';

INSERT INTO dcp_zoningtaxlots.qaqc_frequency (
    SELECT
        count(boroughcode) AS boroughcode,
        count(taxblock) AS taxblock,
        count(taxlot) AS taxlot,
        count(bbl) AS bbl,
        count(zoningdistrict1) AS zoningdistrict1,
        count(zoningdistrict2) AS zoningdistrict2,
        count(zoningdistrict3) AS zoningdistrict3,
        count(zoningdistrict4) AS zoningdistrict4,
        count(commercialoverlay1) AS commercialoverlay1,
        count(commercialoverlay2) AS commercialoverlay2,
        count(specialdistrict1) AS specialdistrict1,
        count(specialdistrict2) AS specialdistrict2,
        count(specialdistrict3) AS specialdistrict3,
        count(limitedheightdistrict) AS limitedheightdistrict,
        count(mihflag) AS mihflag,
        count(mihoption) AS mihoption,
        count(zoningmapnumber) AS zoningmapnumber,
        count(zoningmapcode) AS zoningmapcode,
        count(area) AS area,
        :'VERSION' AS version
    FROM dcp_zoningtaxlots.:"VERSION"
);
