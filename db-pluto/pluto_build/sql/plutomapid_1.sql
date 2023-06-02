UPDATE pluto a
SET plutomapid = '4'
WHERE a.bbl in (
select a.bbl from (
    select a.bbl as bbl, st_union(b.geom) as geom
	from pluto a, dof_shoreline_subdivide b
	WHERE a.plutomapid = '1' 
        and a.geom IS NOT NULL
        and a.geom&&ST_MakeValid(b.geom) 
        and ST_intersects(a.geom, ST_MakeValid(b.geom))
	group by a.bbl) a
	join pluto b
	on a.bbl=b.bbl
	WHERE ST_within(b.geom, a.geom));