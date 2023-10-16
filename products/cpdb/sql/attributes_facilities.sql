-- get geometries for attributes table from facilities database

UPDATE cpdb_dcpattributes a
SET
    geomsource = 'Facilities database',
    geom = c.wkb_geometry
FROM attributes_maprojid_facilities AS b, dcp_facilities AS c
WHERE
    a.maprojid = b.maprojid
    AND b.uid = c.uid;
