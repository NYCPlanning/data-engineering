-- Create a single 500-year flood polygon from combined FIRM and PFIRM records
DROP TABLE IF EXISTS flood_500;
SELECT
    1 AS id,
    wkb_geometry AS wkb_geometry
INTO flood_500
FROM (
    SELECT wkb_geometry
    FROM fema_pfirms2015_100yr
    WHERE fld_zone != 'X'
    UNION
    SELECT wkb_geometry
    FROM fema_firms2007_100yr
    WHERE fld_zone != 'X'
) AS a;

-- Create a single 100-year flood polygon from combined FIRM and PFIRM records
DROP TABLE IF EXISTS flood_100;
SELECT
    1 AS id,
    wkb_geometry AS wkb_geometry
INTO flood_100
FROM (
    SELECT wkb_geometry
    FROM fema_pfirms2015_100yr
    WHERE fld_zone != 'X' AND fld_zone != '0.2 PCT ANNUAL CHANCE FLOOD HAZARD'
    UNION
    SELECT wkb_geometry
    FROM fema_firms2007_100yr
    WHERE fld_zone != 'X' AND fld_zone != '0.2 PCT ANNUAL CHANCE FLOOD HAZARD'
) AS a;

-- combine all parks access geometry
DROP TABLE IF EXISTS park_union;
SELECT
    1 AS id,
    ST_SUBDIVIDE(wkb_geometry) AS wkb_geometry
INTO park_union FROM dpr_park_access_zone;

-- combining blocks and tracts
DROP TABLE IF EXISTS block_tracts;
SELECT
    dcp_censusblocks.geoid,
    bctcb2020,
    dcp_censusblocks.ct2020,
    dcp_censustracts.nta2020,
    dcp_censustracts.ntaname,
    dcp_censustracts.cdta2020,
    dcp_censustracts.cdtaname,
    dcp_censusblocks.borocode,
    dcp_censusblocks.boroname,
    RIGHT(bctcb2020, 10) AS ctcb2020,
    SUBSTRING(bctcb2020, 2, 7) AS ctcbg2020,
    (CASE
        WHEN LEFT(dcp_censusblocks.geoid, 5) = '36061' THEN 'New York'
        WHEN LEFT(dcp_censusblocks.geoid, 5) = '36005' THEN 'Bronx'
        WHEN LEFT(dcp_censusblocks.geoid, 5) = '36047' THEN 'Kings'
        WHEN LEFT(dcp_censusblocks.geoid, 5) = '36081' THEN 'Queens'
        WHEN LEFT(dcp_censusblocks.geoid, 5) = '36085' THEN 'Richmond'
    END) AS county,
    LEFT(dcp_censusblocks.geoid, 5) AS county_fips,
    ST_CENTROID(dcp_censusblocks.wkb_geometry) AS cb2020_centroid_geom
INTO block_tracts
FROM dcp_censusblocks RIGHT JOIN dcp_censustracts
    ON dcp_censusblocks.borocode || dcp_censusblocks.ct2020 = dcp_censustracts.boroct2020;

DROP INDEX IF EXISTS flood_100_wkb_geometry_geom_idx;
CREATE INDEX flood_100_wkb_geometry_geom_idx
ON flood_100 USING gist (wkb_geometry gist_geometry_ops_2d);
DROP INDEX IF EXISTS flood_500_wkb_geometry_geom_idx;
CREATE INDEX flood_500_wkb_geometry_geom_idx
ON flood_500 USING gist (wkb_geometry gist_geometry_ops_2d);
DROP INDEX IF EXISTS park_union_wkb_geometry_geom_idx;
CREATE INDEX park_union_wkb_geometry_geom_idx
ON park_union USING gist (wkb_geometry gist_geometry_ops_2d);
DROP INDEX IF EXISTS block_tracts_cb2020_centroid_geom_geom_idx;
CREATE INDEX block_tracts_cb2020_centroid_geom_geom_idx
ON block_tracts USING gist (cb2020_centroid_geom gist_geometry_ops_2d);

DROP TABLE IF EXISTS geolookup;
SELECT DISTINCT
    block_tracts.*,
    (flood_100.id IS NULL)::int AS fp_100,
    (flood_500.id IS NULL)::int AS fp_500,
    (park_union.id IS NULL)::int AS park_access
INTO geolookup
FROM block_tracts
LEFT JOIN flood_100 ON ST_INTERSECTS(block_tracts.cb2020_centroid_geom, flood_100.wkb_geometry)
LEFT JOIN flood_500 ON ST_INTERSECTS(block_tracts.cb2020_centroid_geom, flood_500.wkb_geometry)
LEFT JOIN park_union ON ST_INTERSECTS(block_tracts.cb2020_centroid_geom, park_union.wkb_geometry);

ALTER TABLE geolookup DROP COLUMN cb2020_centroid_geom;
