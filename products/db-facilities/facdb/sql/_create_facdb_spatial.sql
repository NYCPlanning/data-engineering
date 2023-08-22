DROP TABLE IF EXISTS facdb_spatial;
WITH boundary_geosupport AS (
    SELECT
        a.uid,
        nullif(nullif(geo_1b -> 'result' ->> 'geo_borough_code', ''), '0')::integer AS borocode,
        coundist::text AS council,
        'geosupport' AS boundarysource,
        nullif(geo_1b -> 'result' ->> 'geo_zip_code', '') AS zipcode,
        nullif(geo_1b -> 'result' ->> 'geo_bin', '') AS bin,
        nullif(nullif(geo_1b -> 'result' ->> 'geo_bbl', ''), '0000000000') AS bbl,
        nullif(geo_1b -> 'result' ->> 'geo_city', '') AS city,
        nullif(geo_1b -> 'result' ->> 'geo_commboard', '') AS commboard,
        -- see comment in join clauses below
        nullif(geo_1b -> 'result' ->> 'geo_nta2010', '') AS nta2010, --nullif(geo_1b->'result'->>'geo_council','') as council,
        nullif(geo_1b -> 'result' ->> 'geo_nta2020', '') AS nta2020,
        nullif(geo_1b -> 'result' ->> 'geo_ct2010', '000000') AS ct2010,
        nullif(geo_1b -> 'result' ->> 'geo_ct2020', '000000') AS ct2020,
        nullif(geo_1b -> 'result' ->> 'geo_policeprct', '') AS policeprct,
        nullif(geo_1b -> 'result' ->> 'geo_schooldist', '') AS schooldist
    FROM facdb_base AS a
-- temporary workaround 2023-04: geosupport is returning latest city council districts, but we need a specific version
    -- in future, remove two lines below and uncomment the council row up above
    LEFT JOIN facdb_geom AS g ON a.uid = g.uid
    LEFT JOIN dcp_councildistricts AS b ON st_intersects(b.wkb_geometry, g.geom)
    WHERE
        nullif(geo_1b -> 'result' ->> 'geo_grc', '') IN ('00', '01')
        AND nullif(geo_1b -> 'result' ->> 'geo_grc2', '') IN ('00', '01')
),

boundary_spatial_join AS (
    SELECT
        uid,
        'spatial join' AS boundarysource,
        coalesce(
            (SELECT left(bbl::text, 1)::integer FROM dcp_mappluto_wi AS b WHERE st_intersects(b.wkb_geometry, a.geom)),
            (SELECT borocode::integer FROM dcp_boroboundaries_wi AS b WHERE st_intersects(b.wkb_geometry, a.geom))
        ) AS borocode,
        (SELECT zipcode FROM doitt_zipcodeboundaries AS b WHERE st_intersects(b.wkb_geometry, a.geom) LIMIT 1) AS zipcode,
        (SELECT bin::bigint::text FROM doitt_buildingfootprints AS b WHERE st_intersects(b.wkb_geometry, a.geom) LIMIT 1) AS bin,
        (SELECT bbl::bigint::text FROM dcp_mappluto_wi AS b WHERE st_intersects(b.wkb_geometry, a.geom)) AS bbl,
        (SELECT upper(po_name) FROM doitt_zipcodeboundaries AS b WHERE st_intersects(b.wkb_geometry, a.geom) LIMIT 1) AS city,
        (SELECT borocd::text FROM dcp_cdboundaries AS b WHERE st_intersects(b.wkb_geometry, a.geom)) AS commboard,
        (SELECT ntacode FROM dcp_nta2010 AS b WHERE st_intersects(b.wkb_geometry, a.geom)) AS nta2010,
        (SELECT nta2020 FROM dcp_nta2020 AS b WHERE st_intersects(b.wkb_geometry, a.geom)) AS nta2020,
        (SELECT coundist::text FROM dcp_councildistricts AS b WHERE st_intersects(b.wkb_geometry, a.geom)) AS council,
        (SELECT right(boroct2010::text, 6) FROM dcp_ct2010 AS b WHERE st_intersects(b.wkb_geometry, a.geom)) AS ct2010,
        (SELECT right(boroct2020::text, 6) FROM dcp_ct2020 AS b WHERE st_intersects(b.wkb_geometry, a.geom)) AS ct2020,
        (SELECT precinct::text FROM dcp_policeprecincts AS b WHERE st_intersects(b.wkb_geometry, a.geom)) AS policeprct,
        (SELECT schooldist::text FROM dcp_school_districts AS b WHERE st_intersects(b.wkb_geometry, a.geom)) AS schooldist
    FROM facdb_geom AS a
    WHERE uid NOT IN (SELECT uid FROM boundary_geosupport) AND geom IS NOT NULL
)

SELECT
    uid,
    borocode,
    bbl,
    city,
    commboard,
    nta2010,
    nta2020,
    ct2010,
    ct2020,
    boundarysource,
    nullif(nullif(regexp_replace(left(zipcode, 5), '[^0-9]+', '', 'g'), '0'), '') AS zipcode,
    (CASE WHEN bin LIKE '%000000' THEN NULL ELSE nullif(bin, '') END) AS bin,
    lpad(council, 2, '0') AS council,
    lpad(policeprct, 3, '0') AS policeprct,
    lpad(schooldist, 2, '0') AS schooldist
INTO facdb_spatial
FROM (
    SELECT * FROM boundary_geosupport
    UNION
    SELECT * FROM boundary_spatial_join
) AS a;
