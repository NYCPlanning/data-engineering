DROP TABLE IF EXISTS shoreline_bbl;
SELECT DISTINCT bbl::bigint 
INTO shoreline_bbl
FROM pluto a, dof_shoreline_subdivide b
WHERE st_intersects(a.geom, b.geom);

DROP TABLE IF EXISTS pluto_geom_tmp;
SELECT 
	bbl, 
	st_makevalid(ST_Transform(a.geom, 2263)) as geom_2263,
	st_makevalid(a.geom) as geom_4326,
	(case when bbl::bigint in 
	 	(SELECT bbl::bigint FROM shoreline_bbl)
	then 1 else 0 END) shoreline
INTO pluto_geom_tmp
FROM pluto a;

DROP TABLE IF EXISTS shoreline_bbl;

DROP TABLE IF EXISTS pluto_geom;
WITH
subdivided as (
	SELECT 
        a.bbl,
        b.geom
	FROM pluto_geom_tmp a, dof_shoreline_subdivide b
	WHERE shoreline = 1
	AND st_intersects(a.geom_4326, b.geom)),
subdivided_union as (
	SELECT
        bbl::bigint,
        st_makevalid(st_union(geom)) as geom
	FROM subdivided
	group by bbl),
clipped as (
	SELECT
        a.bbl::bigint,
        ST_Multi(ST_CollectionExtract(ST_Difference(a.geom_4326, b.geom), 3)) as geom
	FROM pluto_geom_tmp a, subdivided_union b
	WHERE a.bbl::bigint = b.bbl::bigint)
select
	a.bbl,
	a.geom_2263,
	a.geom_4326,
	COALESCE(b.geom, a.geom_4326) as clipped_4326,
	COALESCE(st_transform(b.geom, 2263), a.geom_2263) as clipped_2263
INTO pluto_geom
FROM pluto_geom_tmp a
left join clipped b
on a.bbl::bigint=b.bbl::bigint;