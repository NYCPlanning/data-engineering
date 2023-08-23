-- create empty dcp_zoning_taxlot table
DROP TABLE IF EXISTS dcp_zoning_taxlot CASCADE;
CREATE TABLE dcp_zoning_taxlot (
	boroughcode text,
	taxblock text,
	taxlot text,
	bbl text,
	zoningdistrict1 text,
	zoningdistrict2 text,
	zoningdistrict3 text,
	zoningdistrict4 text,
	commercialoverlay1 text,
	commercialoverlay2 text,
	specialdistrict1 text,
	specialdistrict2 text, 
	specialdistrict3 text,
	limitedheightdistrict text,
	mihflag boolean,
	mihoption text,
	zoningmapnumber text,
	zoningmapcode text,
	area double precision,
	inzonechange text
);