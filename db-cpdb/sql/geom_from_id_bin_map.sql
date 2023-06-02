-- Update Attributes table geometries from footprints based on dcp_id_bin_map

UPDATE cpdb_dcpattributes a
SET geom = ST_Centroid(b.wkb_geometry),
    geomsource = 'footprint_script'
FROM doitt_buildingfootprints b,
     dcp_id_bin_map c
WHERE c.bin::bigint = b.bin::bigint AND
      a.maprojid = c.maprojid;