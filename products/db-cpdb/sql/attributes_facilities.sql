-- get geometries for attributes table from facilities database

UPDATE cpdb_dcpattributes a
SET geomsource = 'Facilities database',
    geom=c.wkb_geometry
FROM attributes_maprojid_facilities b, dcp_facilities c 
WHERE a.maprojid = b.maprojid AND
      b.uid = c.uid
;
