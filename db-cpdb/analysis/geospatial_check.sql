DROP TABLE IF EXISTS geospatial_check;
CREATE TABLE IF NOT EXISTS geospatial_check(
    v character varying,
    result character varying
);

DELETE FROM geospatial_check
WHERE v = :'ccp_v';

INSERT INTO geospatial_check (
select :'ccp_v' as v, 	 
        jsonb_agg(t) as result
from (
    select jsonb_agg(json_build_object('projectid', tmp.projectid, 'magencyacro',tmp.magencyacro, 
                              'typecategory', tmp.typecategory,
                              'description', tmp.description)) as values, 
                              'projects_not_within_NYC' as field
	from (SELECT a.projectid, a.magencyacro, a.typecategory, a.description
          FROM cpdb_dcpattributes a, 
               (SELECT ST_Union(wkb_geometry) geom
               FROM dcp_boroboundaries_wi) combined 
          WHERE NOT ST_WITHIN(a.geom, combined.geom)) tmp
    )t
)