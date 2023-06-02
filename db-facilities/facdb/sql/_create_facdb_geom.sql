DROP TABLE IF EXISTS facdb_geom;
SELECT
	uid,
    geom,
    geomsource,
    ST_astext(geom) as wkt,
    ST_X(geom) as longitude,
    ST_Y(geom) as latitude,
    ST_X(ST_Transform(geom, 2263)) as x,
    ST_Y(ST_Transform(geom, 2263)) as y,
    b.wkb_geometry,
	b.geom_1b,
	b.geom_bl,
	b.geom_bn,
	b.geom_pluto,
	b.geom_bldg
INTO facdb_geom
FROM (
	SELECT uid,
	    ST_SetSRID(
	        coalesce(
	            geom_bldg,
	            geom_pluto,
	            geom_bn,
	            geom_bl,
	            geom_1b,
	            wkb_geometry
	        ),
	        4326
	    ) as geom,
	    coalesce(
	        source_bldg,
	        source_pluto,
	        source_bn,
	        source_bl,
	        source_1b,
	        source_wkb
	    ) as geomsource,
	    a.wkb_geometry,
		a.geom_1b,
		a.geom_bl,
		a.geom_bn,
		a.geom_pluto,
		a.geom_bldg
	FROM (
		SELECT
			facdb_base_geom.uid,
			facdb_base_geom.wkb_geometry,
			facdb_base_geom.geo_bbl,
			facdb_base_geom.geo_bin,
			facdb_base_geom.geom_1b,
			facdb_base_geom.geom_bl,
			facdb_base_geom.geom_bn,
			st_centroid(dcp_mappluto_wi.wkb_geometry) as geom_pluto,
			st_centroid(doitt_buildingfootprints.wkb_geometry) as geom_bldg,
			(case when facdb_base_geom.wkb_geometry is not null then 'wkb_geometry' end) as source_wkb,
			(case when geom_1b is not null then '1b' end) as source_1b,
			(case when geom_bl is not null then 'bl' end) as source_bl,
			(case when geom_bn is not null then 'bn' end) as source_bn,
			(case when dcp_mappluto_wi.wkb_geometry is not null then 'pluto bbl centroid' end) as source_pluto,
			(case when doitt_buildingfootprints.wkb_geometry is not null then 'building centroid' end ) as source_bldg
		FROM (
			SELECT
				uid,
				st_centroid(wkb_geometry) as wkb_geometry,
				geo_1b->'result'->>'geo_bbl' as geo_bbl,
				(CASE
				    WHEN geo_1b->'result'->>'geo_bin' IN (
				        '5000000',
				        '4000000',
				        '3000000',
				        '2000000',
				        '1000000'
				    ) THEN NULL
				    ELSE geo_1b->'result'->>'geo_bin'
				END) as geo_bin,
				st_point(
					nullif(geo_1b->'result'->>'geo_longitude', '')::double precision,
					nullif(geo_1b->'result'->>'geo_latitude', '')::double precision
				) as geom_1b,
				st_point(
					nullif(geo_bl->'result'->>'geo_longitude', '')::double precision,
					nullif(geo_bl->'result'->>'geo_latitude', '')::double precision
				) as geom_bl,
				st_point(
					nullif(geo_bn->'result'->>'geo_longitude', '')::double precision,
					nullif(geo_bn->'result'->>'geo_latitude', '')::double precision
				) as geom_bn
			from facdb_base
		) as facdb_base_geom
		LEFT JOIN dcp_mappluto_wi
			ON dcp_mappluto_wi.bbl::bigint::text = facdb_base_geom.geo_bbl
		LEFT JOIN doitt_buildingfootprints
			ON doitt_buildingfootprints.bin::bigint::text = facdb_base_geom.geo_bin
	) a
) b;
