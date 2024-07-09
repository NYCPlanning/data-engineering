-- backfill ungeocoded records with manually researched geoms
WITH all_manual_mappings AS (
    SELECT * FROM dcp_cbbr_manualmappings_points
    UNION ALL
    SELECT * FROM dcp_cbbr_manualmappings_poly
)
UPDATE _cbbr_submissions a
SET
    geom = b.wkb_geometry,
    geo_function = 'Manual_Research'
FROM all_manual_mappings AS b
WHERE
    a.tracking_code = regexp_replace(b.tracking_c, E'[\\n\\r]+', '', 'g')
    AND a.geom IS NULL;
