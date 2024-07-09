-- backfill ungeocoded records with manually researched geoms
WITH regid_lookup AS (
    SELECT
        a.regid,
        a.trackingnum
    FROM old_cbbr_submissions AS a
    INNER JOIN cbbr_submissions AS b
        ON a.trackingnum = b.parent_tracking_code
    WHERE a.regid IS NOT NULL
),
geoms AS (
    SELECT
        a.trackingnum,
        b.geom
    FROM regid_lookup AS a
    INNER JOIN cbbr_geoms AS b
        ON a.regid = b.regid
)
UPDATE cbbr_submissions a
SET
    geom = b.geom,
    geo_function = 'Manual_Research'
FROM geoms AS b
WHERE
    a.parent_tracking_code = b.trackingnum
    AND a.geom IS NULL;
