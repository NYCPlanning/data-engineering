-- backfill ungeocoded records with manually researched geoms
UPDATE _cbbr_submissions a
SET
    geom = b.geom,
    geo_function = 'Manual_Research'
FROM dcp_cbbr_manualmappings AS b
WHERE
    a.unique_id = b.unique_id
    AND a.geom IS NULL;
