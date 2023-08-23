DROP TABLE IF EXISTS facdb_spatial;
with boundary_geosupport as (
	SELECT
		a.uid,
		nullif(nullif(geo_1b->'result'->>'geo_borough_code',''), '0')::integer as borocode,
		nullif(geo_1b->'result'->>'geo_zip_code','') as zipcode,
		nullif(geo_1b->'result'->>'geo_bin','') as bin,
		nullif(nullif(geo_1b->'result'->>'geo_bbl',''), '0000000000') as bbl,
		nullif(geo_1b->'result'->>'geo_city','') as city,
		nullif(geo_1b->'result'->>'geo_commboard','') as commboard,
		nullif(geo_1b->'result'->>'geo_nta2010','') as nta2010,
		nullif(geo_1b->'result'->>'geo_nta2020','') as nta2020,
		-- see comment in join clauses below
		coundist::text as council, --nullif(geo_1b->'result'->>'geo_council','') as council,
		nullif(geo_1b->'result'->>'geo_ct2010','000000') as ct2010,
		nullif(geo_1b->'result'->>'geo_ct2020','000000') as ct2020,
		nullif(geo_1b->'result'->>'geo_policeprct','') as policeprct,
		nullif(geo_1b->'result'->>'geo_schooldist','') as schooldist,
		'geosupport' as boundarysource
	FROM facdb_base a
	-- temporary workaround 2023-04: geosupport is returning latest city council districts, but we need a specific version
	-- in future, remove two lines below and uncomment the council row up above
	LEFT JOIN facdb_geom g ON a.uid = g.uid
	LEFT JOIN dcp_councildistricts b ON st_intersects(b.wkb_geometry, g.geom)
	WHERE nullif(geo_1b->'result'->>'geo_grc','') IN ('00', '01')
	AND nullif(geo_1b->'result'->>'geo_grc2','') IN ('00', '01')
), boundary_spatial_join as (
	SELECT
		uid,
		coalesce(
			(select left(bbl::text, 1)::integer from dcp_mappluto_wi b where st_intersects(b.wkb_geometry, a.geom)),
			(select borocode::integer from dcp_boroboundaries_wi b where st_intersects(b.wkb_geometry, a.geom))
		)as borocode,
		(select zipcode from doitt_zipcodeboundaries b where st_intersects(b.wkb_geometry, a.geom) limit 1) as zipcode,
		(select bin::bigint::text from doitt_buildingfootprints b where st_intersects(b.wkb_geometry, a.geom) limit 1) as bin,
		(select bbl::bigint::text from dcp_mappluto_wi b where st_intersects(b.wkb_geometry, a.geom)) as bbl,
		(select UPPER(po_name) from doitt_zipcodeboundaries b where st_intersects(b.wkb_geometry, a.geom) limit 1) as city,
		(select borocd::text from dcp_cdboundaries b where st_intersects(b.wkb_geometry, a.geom)) as commboard,
		(select ntacode from dcp_nta2010 b where st_intersects(b.wkb_geometry, a.geom)) as nta2010,
		(select nta2020 from dcp_nta2020 b where st_intersects(b.wkb_geometry, a.geom)) as nta2020,
		(select coundist::text from dcp_councildistricts b where st_intersects(b.wkb_geometry, a.geom)) as council,
		(select RIGHT(boroct2010::text, 6) from dcp_ct2010 b where st_intersects(b.wkb_geometry, a.geom)) as ct2010,
		(select RIGHT(boroct2020::text, 6) from dcp_ct2020 b where st_intersects(b.wkb_geometry, a.geom)) as ct2020,
		(select precinct::text from dcp_policeprecincts b where st_intersects(b.wkb_geometry, a.geom)) as policeprct,
		(select schooldist::text from dcp_school_districts b where st_intersects(b.wkb_geometry, a.geom)) as schooldist,
		'spatial join' as boundarysource
	FROM facdb_geom a
	WHERE uid NOT IN (SELECT uid FROM boundary_geosupport) AND geom IS NOT NULL
)
SELECT
	uid,
	borocode,
	NULLIF(NULLIF(regexp_replace(LEFT(zipcode, 5), '[^0-9]+', '', 'g'), '0'), '') as zipcode,
	(CASE WHEN bin LIKE '%000000' THEN NULL ELSE NULLIF(bin, '') END) as bin,
	bbl,
	city,
	commboard,
	nta2010,
	nta2020,
	LPAD(council,2,'0') as council,
	ct2010,
	ct2020,
	LPAD(policeprct,3,'0') as policeprct,
	LPAD(schooldist,2,'0') as schooldist,
	boundarysource
INTO facdb_spatial
FROM (
    SELECT * FROM boundary_geosupport UNION
    SELECT * FROM boundary_spatial_join
) a;
