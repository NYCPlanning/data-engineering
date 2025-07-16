-- Update Attributes table geometries from footprints based on dcp_id_bin_map

UPDATE cpdb_dcpattributes a
SET
    geom = ST_Centroid(b.wkb_geometry),
    geomsource = 'footprint_script'
FROM doitt_buildingfootprints AS b,
    dcp_id_bin_map AS c
WHERE
    c.bin::bigint = b.bin::bigint
    AND a.maprojid = c.maprojid
    AND b.wkb_geometry IS NOT NULL;
