-- Code of 1 means that some portion of the tax lot falls within the 1% annual chance floodplain
-- using defs from here https://snmapmod.snco.us/fmm/document/fema-flood-zone-definitions.pdf

-- for 2007 floodplain
UPDATE pluto a
SET firm07_flag = '1'
FROM (
    SELECT ST_SUBDIVIDE(ST_MAKEVALID(geom)) AS geom
    FROM stg__fema_firms2007_100yr AS b
    WHERE
        b.fld_zone != 'X'
        AND b.fld_zone != '0.2 PCT ANNUAL CHANCE FLOOD HAZARD'
) AS b
WHERE a.geom && b.geom AND ST_INTERSECTS(a.geom, b.geom);

-- for 2015 p floodplain
UPDATE pluto a
SET pfirm15_flag = '1'
FROM (
    SELECT ST_SUBDIVIDE(ST_MAKEVALID(geom)) AS geom
    FROM stg__fema_pfirms2015_100yr AS b
    WHERE
        b.fld_zone != 'X'
        AND b.fld_zone != '0.2 PCT ANNUAL CHANCE FLOOD HAZARD'
) AS b
WHERE a.geom && b.geom AND ST_INTERSECTS(a.geom, b.geom);
