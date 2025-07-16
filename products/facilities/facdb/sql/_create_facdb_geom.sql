DROP TABLE IF EXISTS facdb_geom;

WITH facdb_base_geom AS (
    SELECT
        uid,
        st_centroid(wkb_geometry) AS wkb_geometry,
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
        st_point(
            nullif(geo_1b -> 'result' ->> 'geo_longitude', '')::double precision,
            nullif(geo_1b -> 'result' ->> 'geo_latitude', '')::double precision
        ) AS geom_1b,
        st_point(
            nullif(geo_bl -> 'result' ->> 'geo_longitude', '')::double precision,
            nullif(geo_bl -> 'result' ->> 'geo_latitude', '')::double precision
        ) AS geom_bl,
        st_point(
            nullif(geo_bn -> 'result' ->> 'geo_longitude', '')::double precision,
            nullif(geo_bn -> 'result' ->> 'geo_latitude', '')::double precision
        ) AS geom_bn
    FROM facdb_base
),

geom_sources AS (
    SELECT
        facdb_base_geom.uid,
        facdb_base_geom.wkb_geometry,
        facdb_base_geom.geo_bbl,
        facdb_base_geom.geo_bin,
        facdb_base_geom.geom_1b,
        facdb_base_geom.geom_bl,
        facdb_base_geom.geom_bn,
        st_centroid(dcp_mappluto_wi.wkb_geometry) AS geom_pluto,
        st_centroid(doitt_buildingfootprints.wkb_geometry) AS geom_bldg,
        (CASE WHEN facdb_base_geom.wkb_geometry IS NOT NULL THEN 'wkb_geometry' END) AS source_wkb,
        (CASE WHEN geom_1b IS NOT NULL THEN '1b' END) AS source_1b,
        (CASE WHEN geom_bl IS NOT NULL THEN 'bl' END) AS source_bl,
        (CASE WHEN geom_bn IS NOT NULL THEN 'bn' END) AS source_bn,
        (CASE WHEN dcp_mappluto_wi.wkb_geometry IS NOT NULL THEN 'pluto bbl centroid' END) AS source_pluto,
        (CASE WHEN doitt_buildingfootprints.wkb_geometry IS NOT NULL THEN 'building centroid' END) AS source_bldg
    FROM facdb_base_geom
    LEFT JOIN dcp_mappluto_wi
        ON dcp_mappluto_wi.bbl::bigint::text = facdb_base_geom.geo_bbl
    LEFT JOIN doitt_buildingfootprints
        ON doitt_buildingfootprints.bin::bigint::text = facdb_base_geom.geo_bin
),

coalesced_geoms AS (
    SELECT
        uid,
        st_setsrid(
            coalesce(
                geom_bldg,
                geom_pluto,
                geom_bn,
                geom_bl,
                geom_1b,
                wkb_geometry
            ),
            4326
        ) AS geom,
        coalesce(
            source_bldg,
            source_pluto,
            source_bn,
            source_bl,
            source_1b,
            source_wkb
        ) AS geomsource,
        wkb_geometry,
        geom_1b,
        geom_bl,
        geom_bn,
        geom_pluto,
        geom_bldg
    FROM geom_sources
),

final AS (
    SELECT
        uid,
        geom,
        geomsource,
        st_astext(geom) AS wkt,
        st_x(geom) AS longitude,
        st_y(geom) AS latitude,
        st_x(st_transform(geom, 2263)) AS x,
        st_y(st_transform(geom, 2263)) AS y,
        wkb_geometry,
        geom_1b,
        geom_bl,
        geom_bn,
        geom_pluto,
        geom_bldg
    FROM coalesced_geoms
)

SELECT * INTO facdb_geom FROM final;
