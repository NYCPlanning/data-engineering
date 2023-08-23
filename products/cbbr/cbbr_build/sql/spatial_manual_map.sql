-- Overwrite geoms with manual mapping input

-- Translate fy22 ids back to fy21 ids to use fy21 manual geoms
WITH fy21_fy22_translate AS (
	SELECT b.fy22_unique_id as unique_id,
			a.geom
	FROM manual_geoms."FY21" a
	JOIN fy21_fy22_lookup b
	ON a.unique_id = b.fy21_unique_id
)
UPDATE cbbr_submissions a
SET geom = b.geom,
	geo_function = 'Manual_Research'
FROM fy21_fy22_translate b
WHERE a.unique_id = b.unique_id;

-- Overwrite with fy22 manual geoms
UPDATE cbbr_submissions a
SET geom = b.geom,
	geo_function = 'Manual_Research'
FROM manual_geoms."FY22" b
WHERE a.unique_id = b.unique_id;