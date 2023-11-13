DROP TABLE IF EXISTS _kpdb;
DROP TABLE IF EXISTS _kpdb_projects_projects;
SELECT
    a.*,
    b.project_id,
    COALESCE(b.units_net, a.units_gross) AS units_net,
    ROUND(
        COALESCE(a.prop_within_5_years::decimal, 0)
        * COALESCE(b.units_net, a.units_gross)::decimal
    ) AS within_5_years,
    ROUND(
        COALESCE(a.prop_5_to_10_years::decimal, 0) * b.units_net::decimal
    ) AS from_5_to_10_years,
    ROUND(
        COALESCE(a.prop_after_10_years::decimal, 0) * b.units_net::decimal
    ) AS after_10_years
INTO _kpdb_projects
FROM combined AS a
LEFT JOIN deduped_units AS b
    ON a.record_id = b.record_id
WHERE a.no_classa = '0' OR a.no_classa IS NULL;

UPDATE _kpdb_projects a
SET
    borough
    = CASE
        WHEN a.borough IS NOT NULL THEN a.borough
        WHEN
            a.source = 'DCP Application' AND a.record_id LIKE 'P%'
            THEN
                (
                    '{"M": 1, "X": 2, "K": 3, "Q": 4, "R": 5}'::json
                    -> SUBSTRING(a.record_id, 6, 1)
                )::varchar
        WHEN
            a.source = 'DCP Application'
            THEN
                (
                    '{"M": 1, "X": 2, "K": 3, "Q": 4, "R": 5}'::json
                    -> SUBSTRING(a.record_id, 5, 1)
                )::varchar
        WHEN a.source = 'DOB' THEN SUBSTRING(a.record_id, 1, 1)
    END
WHERE borough IS NULL;
