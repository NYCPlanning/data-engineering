-- export zoning tax lot database with desired headings
DROP TABLE IF EXISTS dcp_zoning_taxlot_export;
CREATE TABLE dcp_zoning_taxlot_export AS (
    SELECT
        boroughcode AS "Borough Code",
        taxlot AS "Tax Lot",
        bbl AS "BBL",
        zoningdistrict1 AS "Zoning District 1",
        zoningdistrict2 AS "Zoning District 2",
        zoningdistrict3 AS "Zoning District 3",
        zoningdistrict4 AS "Zoning District 4",
        commercialoverlay1 AS "Commercial Overlay 1",
        commercialoverlay2 AS "Commercial Overlay 2",
        specialdistrict1 AS "Special District 1",
        specialdistrict2 AS "Special District 2",
        specialdistrict3 AS "Special District 3",
        limitedheightdistrict AS "Limited Height District",
        zoningmapnumber AS "Zoning Map Number",
        zoningmapcode AS "Zoning Map Code",
        trunc(taxblock::numeric) AS "Tax Block"
    FROM dcp_zoning_taxlot
);
