-- Write geocode results (dcp_cpdb_agencyverified_geo) back into dcp_cpdb_agencyverified's geom
-- column. int__dcpattributes_geom_agencyverified.sql independently recomputes this same geom for
-- cpdb_dcpattributes, so this is no longer needed for that -- but
-- sql/analysis/agency_validated_geoms_summary_table.sql still reads dcp_cpdb_agencyverified.geom
-- directly (filtering on IS NOT NULL), so it needs to reflect the geocode fill, not just the
-- bin/bbl gap-fill from attributes_agencyverified_geoms.sql.
UPDATE dcp_cpdb_agencyverified a
SET geom = ST_CENTROID(b.wkb_geometry)
FROM stg__doitt_buildingfootprints AS b, dcp_cpdb_agencyverified_geo AS c
WHERE
    b.bin::bigint::text = c.bin::bigint::text AND a.maprojid = c.maprojid AND c.bin IS NOT null
    AND b.wkb_geometry IS NOT null;

UPDATE dcp_cpdb_agencyverified a
SET geom = ST_SETSRID(ST_MAKEPOINT(c.lon::double precision, c.lat::double precision), 4326)
FROM dcp_cpdb_agencyverified_geo AS c
WHERE a.maprojid = c.maprojid AND c.lon IS NOT null AND c.lat IS NOT null;
