SELECT
    wkb_geometry AS geom,
    section,
    zoning_map,
    quartersec,
    westmap,
    eastmap,
    northmap,
    southmap,
    northwestm,
    northeastm,
    southwestm,
    southeastm,
    shape_leng,
    shape_area
FROM dcp_zoningmapindex;
