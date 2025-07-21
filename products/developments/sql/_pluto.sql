/*
IN PREVIOUS VERSION:
    pluto_merge.sql
*/
DROP TABLE IF EXISTS pluto_devdb CASCADE;
SELECT
    a.*,
    b.version AS pluto_version,
    b.bbl AS mappluto_bbl,
    b.unitsres AS pluto_unitres,
    b.bldgarea AS pluto_bldgsf,
    b.comarea AS pluto_comsf,
    b.officearea AS pluto_offcsf,
    b.retailarea AS pluto_retlsf,
    b.resarea AS pluto_ressf,
    b.yearbuilt AS pluto_yrbuilt,
    b.yearalter1 AS pluto_yralt1,
    b.yearalter2 AS pluto_yralt2,
    b.bldgclass AS pluto_bldgcls,
    b.landuse AS pluto_landuse,
    b.ownertype AS pluto_owntype,
    b.ownername AS pluto_owner,
    b.condono AS pluto_condo,
    b.numbldgs AS pluto_bldgs,
    b.numfloors AS pluto_floors,
    b.firm07_fla AS pluto_firm07,
    b.pfirm15_fl AS pluto_pfirm15,
    b.histdist AS pluto_histdst,
    b.landmark AS pluto_landmk
INTO pluto_devdb
FROM init_devdb AS a
LEFT JOIN dcp_mappluto_wi AS b
    ON a.geo_bbl = b.bbl::bigint::text;
CREATE INDEX pluto_devdb_job_number_idx ON pluto_devdb (job_number);
