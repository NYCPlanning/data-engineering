CREATE OR REPLACE TABLE ap_police AS
SELECT
    a.atomicid,
    CASE WHEN a.centroid_inside THEN 'centroid' ELSE 'majority_area' END AS method,
    pr.value AS police_precinct,
    pr.share AS police_precinct_share,
    pr.globalid AS nypdprecinct_globalid,
    pb.value AS patrol_borough,
    pb.share AS patrol_borough_share,
    pb.globalid AS nypdpatrolborough_globalid,
    se.value AS police_sector,
    se.share AS police_sector_share,
    se.globalid AS nypdbeat_globalid
FROM police_aps AS a
LEFT JOIN police_precinct AS pr ON a.atomicid = pr.atomicid
LEFT JOIN police_patrol_borough AS pb ON a.atomicid = pb.atomicid
LEFT JOIN police_sector AS se ON a.atomicid = se.atomicid;
