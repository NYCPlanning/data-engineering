CREATE OR REPLACE TABLE police_cmp AS
WITH p AS (SELECT * FROM read_parquet('{data}/pg_ap_police.parquet'))
SELECT
    count(*) FILTER (WHERE d.atomicid IS NULL) AS pg_only,
    count(*) FILTER (WHERE p.atomicid IS NULL) AS duck_only,
    count(*) FILTER (WHERE d.method IS DISTINCT FROM p.method) AS method_diff,
    count(*) FILTER (WHERE d.police_precinct IS DISTINCT FROM p.police_precinct) AS precinct_diff,
    count(*) FILTER (WHERE d.patrol_borough IS DISTINCT FROM p.patrol_borough) AS patrol_borough_diff,
    count(*) FILTER (WHERE d.police_sector IS DISTINCT FROM p.police_sector) AS sector_diff,
    count(*) FILTER (WHERE d.nypdprecinct_globalid IS DISTINCT FROM p.nypdprecinct_globalid
        OR d.nypdpatrolborough_globalid IS DISTINCT FROM p.nypdpatrolborough_globalid
        OR d.nypdbeat_globalid IS DISTINCT FROM p.nypdbeat_globalid) AS globalid_diff,
    max(abs(d.police_precinct_share - p.police_precinct_share::DOUBLE)) AS max_precinct_share_diff,
    max(abs(d.police_sector_share - p.police_sector_share::DOUBLE)) AS max_sector_share_diff
FROM ap_police AS d
FULL OUTER JOIN p ON d.atomicid = p.atomicid;
