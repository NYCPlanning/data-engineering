-- backfill ungeocoded records with manually researched geoms
WITH geoms AS(
	WITH regid_lookup AS(
		SELECT a.regid, a.trackingnum
		FROM old_cbbr_submissions a
		RIGHT JOIN cbbr_submissions b
		ON a.trackingnum = b.parent_tracking_code
		WHERE a.regid IS NOT NULL AND a.trackingnum IS NOT NULL
	)
	SELECT a.trackingnum, b.geom FROM regid_lookup a
	INNER JOIN cbbr_geoms b
	ON a.regid = b.regid
)
UPDATE cbbr_submissions a
SET geom = b.geom,
	geo_function = 'Manual_Research'
FROM geoms b
WHERE a.parent_tracking_code = b.trackingnum
AND a.geom IS NULL;
