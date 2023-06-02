/*
IN PREVIOUS VERSION: 
    pluto_merge.sql
*/
DROP TABLE IF EXISTS PLUTO_devdb CASCADE;
SELECT 
    a.*,
    b.version as pluto_version,
	b.bbl as mpluto_bbl,
	b.unitsres as pluto_unitres,
	b.bldgarea as pluto_bldgsf,
	b.comarea as pluto_comsf,
    b.officearea as pluto_offcsf,
	b.retailarea as pluto_retlsf,
	b.resarea as pluto_ressf,
	b.yearbuilt as pluto_yrbuilt,
	b.yearalter1 as pluto_yralt1,
	b.yearalter2 as pluto_yralt2,
	b.bldgclass as pluto_bldgcls,
	b.landuse as pluto_landuse,
	b.ownertype as pluto_owntype,
    b.ownername as pluto_owner,
	b.condono as pluto_condo,
	b.numbldgs as pluto_bldgs,
	b.numfloors as pluto_floors,
	b.firm07_fla as pluto_firm07,
	b.pfirm15_fl as pluto_pfirm15,
	b.histdist as pluto_histdst,
	b.landmark as pluto_landmk
INTO PLUTO_devdb
FROM INIT_devdb a
LEFT JOIN dcp_mappluto b
ON a.geo_bbl = b.bbl::bigint::text;
CREATE INDEX PLUTO_devdb_job_number_idx ON PLUTO_devdb(job_number);
