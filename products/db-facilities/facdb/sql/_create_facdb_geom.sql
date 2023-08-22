DROP TABLE IF EXISTS facdb_geom;
SELECT
    uid,
    geom,
    geomsource,
    b.wkb_geometry,
    b.geom_1b,
    b.geom_bl,
    b.geom_bn,
    b.geom_pluto,
    b.geom_bldg,
    ST_ASTEXT(geom) AS wkt,
    ST_X(geom) AS longitude,
    ST_Y(geom) AS latitude,
    ST_X(ST_TRANSFORM(geom, 2263)) AS x,
    ST_Y(ST_TRANSFORM(geom, 2263)) AS y
INTO facdb_geom
FROM (
    SELECT
        uid,
        a.wkb_geometry,
        a.geom_1b,
        a.geom_bl,
        a.geom_bn,
        a.geom_pluto,
        a.geom_bldg,
        ST_SETSRID(
            COALESCE(
                geom_bldg,
                geom_pluto,
                geom_bn,
                geom_bl,
                geom_1b,
                wkb_geometry
            ),
            4326
        ) AS geom,
        COALESCE(
            source_bldg,
            source_pluto,
            source_bn,
            source_bl,
            source_1b,
            source_wkb
        ) AS geomsource
    FROM (
        SELECT
            facdb_base_geom.uid,
            facdb_base_geom.wkb_geometry,
            facdb_base_geom.geo_bbl,
            facdb_base_geom.geo_bin,
            facdb_base_geom.geom_1b,
            facdb_base_geom.geom_bl,
            facdb_base_geom.geom_bn,
            ST_CENTROID(dcp_mappluto_wi.wkb_geometry) AS geom_pluto,
            ST_CENTROID(doitt_buildingfootprints.wkb_geometry) AS geom_bldg,
            (CASE WHEN facdb_base_geom.wkb_geometry IS NOT NULL THEN 'wkb_geometry' END) AS source_wkb,
            (CASE WHEN geom_1b IS NOT NULL THEN '1b' END) AS source_1b,
            (CASE WHEN geom_bl IS NOT NULL THEN 'bl' END) AS source_bl,
            (CASE WHEN geom_bn IS NOT NULL THEN 'bn' END) AS source_bn,
            (CASE WHEN dcp_mappluto_wi.wkb_geometry IS NOT NULL THEN 'pluto bbl centroid' END) AS source_pluto,
            (CASE WHEN doitt_buildingfootprints.wkb_geometry IS NOT NULL THEN 'building centroid' END) AS source_bldg
        FROM (
            SELECT
                uid,
                ST_CENTROID(wkb_geometry) AS wkb_geometry,
                geo_1b -> 'result' ->> 'geo_bbl' AS geo_bbl,
                (CASE
                    WHEN geo_1b -> 'result' ->> 'geo_bin' IN (
                        '5000000',
                        '4000000',
                        '3000000',
                        '2000000',
                        '1000000'
                    ) THEN NULL
                    ELSE geo_1b -> 'result' ->> 'geo_bin'
                END) AS geo_bin,
                ST_POINT(
                    NULLIF(geo_1b -> 'result' ->> 'geo_longitude', '')::double precision,
                    NULLIF(geo_1b -> 'result' ->> 'geo_latitude', '')::double precision
                ) AS geom_1b,
                ST_POINT(
                    NULLIF(geo_bl -> 'result' ->> 'geo_longitude', '')::double precision,
                    NULLIF(geo_bl -> 'result' ->> 'geo_latitude', '')::double precision
                ) AS geom_bl,
                ST_POINT(
                    NULLIF(geo_bn -> 'result' ->> 'geo_longitude', '')::double precision,
                    NULLIF(geo_bn -> 'result' ->> 'geo_latitude', '')::double precision
                ) AS geom_bn
            FROM facdb_base
        ) AS facdb_base_geom
        LEFT JOIN dcp_mappluto_wi
            ON dcp_mappluto_wi.bbl::bigint::text = facdb_base_geom.geo_bbl
        LEFT JOIN doitt_buildingfootprints
            ON doitt_buildingfootprints.bin::bigint::text = facdb_base_geom.geo_bin
    ) AS a
) AS b;
