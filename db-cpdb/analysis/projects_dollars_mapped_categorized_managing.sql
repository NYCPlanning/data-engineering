-- Managing agency: Summary table of # of project and total $s in CPDB and mapped
DROP TABLE IF EXISTS cpdb_summarystats_magency;
CREATE TABLE cpdb_summarystats_magency as (
WITH projects AS (
	SELECT a.*, b.totalcost FROM cpdb_dcpattributes a
	LEFT JOIN cpdb_projects b
	ON a.maprojid=b.maprojid)

SELECT a.magencyacro, 
	COUNT(*) AS totalcount, 
	SUM(totalcost) AS totalcommit, 
	e.count AS mapped, 
	e.sum AS mappedcommit,
	k.count AS mappedviamatch, 
	k.sum AS mappedviamatchcommit, 
	l.count AS mappedviaalgorithm, 
	l.sum AS mappedviaalgorithmcommit, 
	m.count AS mappedviaresearch, 
	m.sum AS mappedviaresearchcommit,
	b.count AS fixedasset, 
	b.sum AS fixedassetcommit, 
	f.count AS fixedassetmapped, 
	f.sum AS fixedassetmappedcommit,
	c.count AS lumpsum, 
	c.sum AS lumpsumcommit, 
	g.count AS lumpsummapped, 
	g.sum AS lumpsummappedcommit,
	d.count AS ittvehequ, 
	d.sum AS ittvehequcommit, 
	h.count AS ittvehequmapped, 
	h.sum AS ittvehequmappedcommit,
	i.count AS unknowntype, 
	i.sum AS unknowntypecommit, 
	j.count AS unknownmapped, 
	j.sum AS unknownmappedcommit
FROM projects a

LEFT JOIN (
	SELECT magencyacro, 
		COUNT(*), 
		SUM(totalcost) 
	FROM projects 
	WHERE typecategory = 'Fixed Asset' 
	GROUP BY magencyacro
	) AS b
ON a.magencyacro = b.magencyacro

LEFT JOIN (
	SELECT magencyacro, 
		COUNT(*), 
		SUM(totalcost) 
	FROM projects 
	WHERE typecategory = 'Lump Sum' 
	GROUP BY magencyacro
	) AS c
ON a.magencyacro = c.magencyacro

LEFT JOIN (
	SELECT magencyacro, 
		COUNT(*), 
		SUM(totalcost) 
	FROM projects 
	WHERE typecategory = 'ITT, Vehicles, and Equipment' 
	GROUP BY magencyacro
	) AS d
ON a.magencyacro = d.magencyacro

LEFT JOIN (
	SELECT magencyacro, 
		COUNT(*), 
		SUM(totalcost) 
	FROM projects 
	WHERE geom IS NOT NULL 
	GROUP BY magencyacro
	) AS e
ON a.magencyacro = e.magencyacro

LEFT JOIN (
	SELECT magencyacro, 
		COUNT(*), 
		SUM(totalcost)
	FROM projects 
	WHERE typecategory = 'Fixed Asset' 
		AND geom IS NOT NULL 
	GROUP BY magencyacro
	) AS f
ON a.magencyacro = f.magencyacro

LEFT JOIN (
	SELECT magencyacro, 
		COUNT(*),
		SUM(totalcost) 
	FROM projects 
	WHERE typecategory = 'Lump Sum' 
		AND geom IS NOT NULL 
	GROUP BY magencyacro
	) AS g
ON a.magencyacro = g.magencyacro

LEFT JOIN (
	SELECT magencyacro, 
		COUNT(*), 
		SUM(totalcost) 
	FROM projects 
	WHERE typecategory = 'ITT, Vehicles, and Equipment' 
		AND geom IS NOT NULL 
	GROUP BY magencyacro
	) AS h
ON a.magencyacro = h.magencyacro

LEFT JOIN (
	SELECT magencyacro, 
		COUNT(*), 
		SUM(totalcost) 
	FROM projects 
	WHERE typecategory IS NULL 
	GROUP BY magencyacro
	) AS i
ON a.magencyacro = i.magencyacro

LEFT JOIN (
	SELECT magencyacro, 
		COUNT(*), 
		SUM(totalcost) 
	FROM projects 
	WHERE typecategory IS NULL 
		AND geom IS NOT NULL 
	GROUP BY magencyacro
	) AS j
ON a.magencyacro = j.magencyacro

LEFT JOIN (
	SELECT magencyacro, 
		COUNT(*), 
		SUM(totalcost) 
	FROM projects 
	WHERE (
		geomsource = 'ddc' 
		OR geomsource = 'dot' 
		OR geomsource = 'agency'
		OR geomsource = 'dpr'
		OR geomsource = 'edc'
		OR dataname = 'dpr_capitalprojects'
		) 
		AND geom IS NOT NULL
	GROUP BY magencyacro) k
ON a.magencyacro = k.magencyacro

LEFT JOIN (
	SELECT magencyacro, 
		COUNT(*), 
		SUM(totalcost) 
	FROM projects
	WHERE (
		geomsource = 'Facilities database' 
		OR geomsource = 'Algorithm' 
		OR dataname = 'dpr_parksproperties'
		) 
		AND geom IS NOT NULL 
	GROUP BY magencyacro) l
ON a.magencyacro = l.magencyacro

LEFT JOIN (
	SELECT magencyacro, 
		COUNT(*),
		SUM(totalcost) 
	FROM projects 
	WHERE (
		geomsource = 'DCP Sprint' 
		OR geomsource = 'AD Sprint' 
		OR geomsource = 'DCP_geojson' 
		OR geomsource = 'footprint_script'
		) 
		AND geom IS NOT NULL 
	GROUP BY magencyacro) m
ON a.magencyacro = m.magencyacro

GROUP BY a.magencyacro, b.count, b.sum, c.count, c.sum, d.count, d.sum, e.count, e.sum, f.count, f.sum, g.count, g.sum, h.count, h.sum, i.count, i.sum, j.count, j.sum, k.count, k.sum, l.count, l.sum, m.count, m.sum
ORDER BY totalcount DESC);

