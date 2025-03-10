DROP TABLE IF EXISTS geospatial_check;
CREATE TABLE IF NOT EXISTS geospatial_check(
    result character varying
);

INSERT INTO geospatial_check (
select  jsonb_agg(t) as result
from (
    select jsonb_agg(json_build_object('uid', tmp.uid, 'hnum', tmp.HNUM, 'sname', tmp.sname, 
    'adress', tmp.address, 'parcelname', tmp.PARCELNAME, 'agency', tmp.AGENCY, 
    'latitude', tmp.latitude,'longitude', tmp.longitude, 'v', tmp.v)) as values, 
                              'projects_not_within_NYC' as field
	from (SELECT a.uid, a.HNUM, a.sname, a.address, a.PARCELNAME, a.AGENCY, a.latitude, a.longitude, a.data_library_version as v
          FROM dcp_colp a, 
               (SELECT ST_Union(wkb_geometry) geom
               FROM dcp_boroboundaries_wi) combined 
          WHERE NOT ST_WITHIN(a.geom, combined.geom)) tmp
    )t
);

INSERT INTO geospatial_check (
select  jsonb_agg(t) as result
from (
    select jsonb_agg(json_build_object('uid', tmp.uid, 'borough', tmp.borough, 'cd', tmp.cd,  
    'bbl', tmp.bbl, 'v', tmp.v)) as values, 
                              'projects_inconsistent_geographies' as field
	from (
        select uid, borough, cd, bbl, data_library_version as v
        FROM dcp_colp
        WHERE borough <> LEFT(cd::TEXT, 1)
        OR borough <> LEFT(bbl::TEXT, 1)
    ) tmp
    )t
);
