-- Sponsor agency: Summary table of # of project and total $s in CPDB and mapped
DROP TABLE IF EXISTS cpdb_summarystats_sagency;
CREATE TABLE cpdb_summarystats_sagency AS (
WITH projects AS (
	SELECT a.*, 
		b.sagencyacro, 
		b.totalcost::double precision
	FROM cpdb_dcpattributes a
	LEFT JOIN (
		SELECT DISTINCT maprojid, 
			sagencyacro, 
			SUM(totalcost::double precision) AS totalcost 
		FROM cpdb_budgets
 		GROUP BY maprojid, sagencyacro
		) b
	ON a.maprojid=b.maprojid)

SELECT a.sagencyacro, 
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
	SELECT sagencyacro, 
		COUNT(*), 
		SUM(totalcost) 
	FROM projects 
	WHERE typecategory = 'Fixed Asset' 
	GROUP BY sagencyacro
	) AS b
ON a.sagencyacro = b.sagencyacro

LEFT JOIN (
	SELECT sagencyacro, 
		COUNT(*), 
		SUM(totalcost) 
	FROM projects 
	WHERE typecategory = 'Lump Sum' 
	GROUP BY sagencyacro
	) AS c
ON a.sagencyacro = c.sagencyacro

LEFT JOIN (
	SELECT sagencyacro, 
		COUNT(*), 
		SUM(totalcost) 
	FROM projects 
	WHERE typecategory = 'ITT, Vehicles, and Equipment' 
	GROUP BY sagencyacro
	) AS d
ON a.sagencyacro = d.sagencyacro

LEFT JOIN (
	SELECT sagencyacro, 
		COUNT(*), 
		SUM(totalcost) 
	FROM projects 
	WHERE geom IS NOT NULL 
	GROUP BY sagencyacro
	) AS e
ON a.sagencyacro = e.sagencyacro

LEFT JOIN (
	SELECT sagencyacro, 
		COUNT(*), 
		SUM(totalcost)
	FROM projects 
	WHERE typecategory = 'Fixed Asset' 
		AND geom IS NOT NULL 
	GROUP BY sagencyacro
	) AS f
ON a.sagencyacro = f.sagencyacro

LEFT JOIN (
	SELECT sagencyacro, 
		COUNT(*),
		SUM(totalcost) 
	FROM projects 
	WHERE typecategory = 'Lump Sum' 
		AND geom IS NOT NULL 
	GROUP BY sagencyacro
	) AS g
ON a.sagencyacro = g.sagencyacro

LEFT JOIN (
	SELECT sagencyacro, 
		COUNT(*), 
		SUM(totalcost) 
	FROM projects 
	WHERE typecategory = 'ITT, Vehicles, and Equipment' 
		AND geom IS NOT NULL 
	GROUP BY sagencyacro
	) AS h
ON a.sagencyacro = h.sagencyacro

LEFT JOIN (
	SELECT sagencyacro, 
		COUNT(*), 
		SUM(totalcost) 
	FROM projects 
	WHERE typecategory IS NULL 
	GROUP BY sagencyacro
	) AS i
ON a.sagencyacro = i.sagencyacro

LEFT JOIN (
	SELECT sagencyacro, 
		COUNT(*), 
		SUM(totalcost) 
	FROM projects 
	WHERE typecategory IS NULL 
		AND geom IS NOT NULL 
	GROUP BY sagencyacro
	) AS j
ON a.sagencyacro = j.sagencyacro

LEFT JOIN (
	SELECT sagencyacro, 
		COUNT(*), 
		SUM(totalcost) 
	FROM projects 
	WHERE (
		geomsource = 'ddc' 
		OR geomsource = 'dot' 
		OR geomsource = 'agency'
		OR geomsource = 'dpr'
		OR geomsource = 'edc'
		OR dataname = 'dpr_capitalprojects_geo'
		) 
		AND geom IS NOT NULL
	GROUP BY sagencyacro) k
ON a.sagencyacro = k.sagencyacro

LEFT JOIN (
	SELECT sagencyacro, 
		COUNT(*), 
		SUM(totalcost) 
	FROM projects
	WHERE (
		geomsource = 'Facilities database' 
		OR geomsource = 'Algorithm' 
		OR dataname = 'dpr_parksproperties'
		) 
		AND geom IS NOT NULL 
	GROUP BY sagencyacro) l
ON a.sagencyacro = l.sagencyacro

LEFT JOIN (
	SELECT sagencyacro, 
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
	GROUP BY sagencyacro) m
ON a.sagencyacro = m.sagencyacro

GROUP BY a.sagencyacro, b.count, b.sum, c.count, c.sum, d.count, d.sum, e.count, e.sum, f.count, f.sum, g.count, g.sum, h.count, h.sum, i.count, i.sum, j.count, j.sum, k.count, k.sum, l.count, l.sum, m.count, m.sum
ORDER BY totalcount DESC
);
